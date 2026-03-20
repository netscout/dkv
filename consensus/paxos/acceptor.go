package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"dkv/storage/wal"
	pb "dkv/transport/proto/dkvpb"

	"google.golang.org/protobuf/proto"
)

// 멀티 paxos 슬롯을 관리하는 acceptor
type Acceptor struct {
	mu     sync.Mutex
	nodeID uint32
	wal    *wal.WAL // 테스트 환경에선 nil 처리
	log    *slog.Logger

	// 약속된 투표(제안 번호) 상태, 글로벌 상태(모든 멀티 paxos 슬롯에 대해 동일)
	promisedBallot *pb.Ballot

	// 슬롯 번호 -> 슬롯 상태
	// 각 슬롯은 독립적인 paxos 인스턴스
	slots map[uint64]*AcceptorSlot
}

// 각 슬롯은 독립적인 paxos 인스턴스
type AcceptorSlot struct {
	// 수락한 투표(제안 번호) 상태
	AcceptedBallot *pb.Ballot
	// 수락한 값
	AcceptedValue *pb.Command
}

func NewAcceptor(nodeID uint32, w *wal.WAL, logger *slog.Logger) *Acceptor {
	return &Acceptor{
		nodeID:         nodeID,
		wal:            w,
		log:            logger,
		promisedBallot: &pb.Ballot{}, // 초기 값이 0인 투표(Number: 0, NodeId: 0)를 생성
		slots:          make(map[uint64]*AcceptorSlot),
	}
}

// 슬롯 번호에 해당하는 슬롯 상태를 가져오거나, 슬롯이 비어있다면 새로 생성(lazy initialization)
func (a *Acceptor) getSlot(slot uint64) *AcceptorSlot {
	s, ok := a.slots[slot]
	if !ok {
		s = &AcceptorSlot{
			AcceptedBallot: &pb.Ballot{}, // 초기 값이 0인 투표(Number: 0, NodeId: 0)를 생성
		}
		a.slots[slot] = s
	}
	return s
}

// acceptor 상태를 WAL에 저장
func (a *Acceptor) persist(slot uint64, slotState *AcceptorSlot) error {
	if a.wal == nil {
		return nil // 테스트 환경에서는 무시
	}
	record := &pb.WALRecord{
		Type: pb.WALRecord_ACCEPTOR_STATE,
		AcceptorState: &pb.AcceptorStateRecord{
			Slot:           slot,
			PromisedBallot: a.promisedBallot,
			AcceptedBallot: slotState.AcceptedBallot,
			AcceptedValue:  slotState.AcceptedValue,
		},
	}
	data, err := proto.Marshal(record) // protobuf -> bytes 변환
	if err != nil {
		return err
	}
	return a.wal.Append(data)
}

// Prepare 요청을 처리
func (a *Acceptor) HandlePrepare(_ context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	/*
		이미 더 높은 ballot에 수락을 약속했다면 거절
		하지만, 동일한 ballot에 대해서는 거절하지 않고 계속 진행한다. -> ex: 네트워크 장애 등으로 인해 같은 Proposer가 같은 ballot을 다시 보내는 경우
	*/
	if BallotGreaterThan(a.promisedBallot, req.Ballot) {
		a.log.Debug("prepare: ballot 부족으로 거부",
			"reqBallot", BallotString(req.Ballot),
			"promisedBallot", BallotString(a.promisedBallot))
		return &pb.PrepareResponse{
			Ok:             false,
			PromisedBallot: BallotClone(a.promisedBallot),
		}, nil
	}

	// 새로운 ballot에 수락을 약속(글로벌) -> 이후 더 낮은 ballot는 모두 거절
	// req는 protobuf 메세지의 포인터 이므로, 그대로 a.promisedBallot에 할당하면, 나중에 요청 객체가 재사용되거나 가비지 컬렉션될 때 문제가 생길 수 있다.
	// 따라서, 복사본을 만들어서 사용한다.
	a.promisedBallot = BallotClone(req.Ballot)

	/*
		아직 commit되지 않았지만, 수락을 약속한 값을 모아서 proposer에게 전달한다.

		멀티 paxos에서 리더가 여러 슬롯에 대해 값을 Accept하고 커밋하기 전에 죽었다면,
		새 Proposer는 Phase 1에서 각 Acceptor로부터 모든 슬롯의 수락 이력을 모아야
		각 슬롯에 대해 올바른 값 채택을 할 수 있다.
	*/
	resp := &pb.PrepareResponse{
		Ok:              true,
		AcceptedEntries: make([]*pb.AcceptedEntry, 0), // 빈 슬라이스 생성(크기를 지정하면 각 요소는 nil로 초기화 되므로, 명시적으로 빈 슬라이스를 만든다.)
	}
	for slotNum, slotState := range a.slots { // Acceptor가 알고 있는 모든 슬롯을 순회한다.
		if slotState.AcceptedValue != nil { // 수락된 값이 있는 경우에만 포함한다.
			resp.AcceptedEntries = append(resp.AcceptedEntries, &pb.AcceptedEntry{
				Slot:    slotNum,                               // 슬롯 번호
				Ballot:  BallotClone(slotState.AcceptedBallot), // 수락 시의 ballot
				Command: slotState.AcceptedValue,               // 수락한 값
			})
		}
	}

	// 응답을 전송하기 전 상태 저장 -> 현재는 0번 슬롯만 저장하지만, 추후 멀티 paxos에서는 모든 슬롯을 저장하도록 수정해야 한다!
	if a.wal != nil {
		s := a.getSlot(0) // 0번 슬롯의 존재를 확인
		if err := a.persist(0, s); err != nil {

			return nil, fmt.Errorf("persist acceptor state: %w", err)
		}
	}

	a.log.Debug("prepare: 약속 완료",
		"ballot", BallotString(req.Ballot),
		"acceptedEntries", len(resp.AcceptedEntries))
	return resp, nil
}

func (a *Acceptor) HandleAccept(_ context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	/*
		이미 더 높은 ballot에 수락을 약속했다면 거절
		하지만, 동일한 ballot에 대해서는 거절하지 않고 계속 진행한다. -> ex: 네트워크 장애 등으로 인해 같은 Proposer가 같은 ballot을 다시 보내는 경우
	*/
	if BallotGreaterThan(a.promisedBallot, req.Ballot) {
		a.log.Debug("accept: ballot 부족으로 거부",
			"slot", req.Slot,
			"reqBallot", BallotString(req.Ballot),
			"promisedBallot", BallotString(a.promisedBallot))
		return &pb.AcceptResponse{
			Ok:             false,
			PromisedBallot: BallotClone(a.promisedBallot),
		}, nil
	}

	// Accept: 상태 업데이트
	a.promisedBallot = BallotClone(req.Ballot) // Accept가 Prepare 없이 직접 올 수도 있으므로, 다시 업데이트한다.
	slotState := a.getSlot(req.Slot)
	slotState.AcceptedBallot = BallotClone(req.Ballot) // 수락한 ballot을 기록한다.
	slotState.AcceptedValue = req.Command              // 수락한 값을 기록한다.

	// 상태 저장
	if err := a.persist(req.Slot, slotState); err != nil {
		return nil, fmt.Errorf("persist acceptor state: %w", err)
	}

	a.log.Debug("accept: 수락 완료",
		"slot", req.Slot,
		"ballot", BallotString(req.Ballot),
		"key", req.Command.Key)
	return &pb.AcceptResponse{Ok: true}, nil
}

// 장애 발생 후 재시작시 슬롯 상태를 복원
func (a *Acceptor) RestoreSlot(rec *pb.AcceptorStateRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// 수락을 약속한 글로벌 투표 상태를 복원
	if rec.PromisedBallot != nil && BallotGreaterThan(rec.PromisedBallot, a.promisedBallot) {
		a.promisedBallot = rec.PromisedBallot
	}

	// 슬롯 상태를 복원
	slotState := a.getSlot(rec.Slot)
	if rec.AcceptedBallot != nil {
		slotState.AcceptedBallot = rec.AcceptedBallot
	}
	slotState.AcceptedValue = rec.AcceptedValue
	a.log.Debug("wal: acceptor 슬롯 복원",
		"slot", rec.Slot,
		"promisedBallot", BallotString(rec.PromisedBallot),
		"acceptedBallot", BallotString(rec.AcceptedBallot))
}
