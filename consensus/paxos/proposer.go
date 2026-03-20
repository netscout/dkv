package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"dkv/transport"
	pb "dkv/transport/proto/dkvpb"
)

/*
각 노드의 투표권을 의미하는 Ballot(제안 번호 + 노드 ID)을 관리하는 구조체
그외의 필드는 MultiPaxos에서 사용되지 않으며, 기본 구현을 테스트 하기 위한 용도로 single_decree_test.go에서만 사용된다.
*/
type Proposer struct {
	mu        sync.Mutex
	nodeID    uint32
	ballot    *pb.Ballot          // 현재 제안 번호(Number + NodeId)
	peers     []uint32            // 자기 자신을 포함한 모든 노드 ID
	majority  int                 // 과반수 수
	transport transport.Transport // 노드 간 통신 계층
	log       *slog.Logger        // 로깅 계층
}

func NewProposer(nodeID uint32, peers []uint32, t transport.Transport, logger *slog.Logger) *Proposer {
	return &Proposer{
		nodeID:    nodeID,
		ballot:    &pb.Ballot{Number: 0, NodeId: nodeID},
		peers:     peers,
		majority:  len(peers)/2 + 1,
		transport: t,
		log:       logger,
	}
}

// 새로운 명령을 합의 과정에 제안하는 함수(테스트 전용)
// single_decree_test.go에서 기본 Paxos 구현 테스트에만 사용되며,
// MultiPaxosNode에서는 사용되지 않는다.
// 참고: 이 메서드의 로그 메시지는 영어로 유지한다 (테스트 전용 코드이므로 한국어 통일 대상 제외).
func (p *Proposer) Propose(ctx context.Context, slot uint64, cmd *pb.Command) (*pb.Command, error) {
	p.mu.Lock()
	p.ballot.Number++
	ballot := BallotClone(p.ballot) // 작업 수행 도중에 다른 goroutine이 현재 처리해야 할 ballot을 수정하지 못하도록 복사본을 만든다.
	p.mu.Unlock()

	// 1 단계: Prepare

	p.log.Info("proposer: start prepare", "slot", slot, "ballot", ballot)

	type prepResult struct {
		resp *pb.PrepareResponse
		err  error
	}
	prepareCh := make(chan prepResult, len(p.peers)) // 채널의 버퍼 크기를 노드 수와 같게 설정

	// 각 노드에 대해 goroutine을 생성하여 병렬로 Prepare 요청을 전송한다. -> 채널의 버퍼 크기가 노드 수와 같으므로, 블로킹 없이 결과를 전송할 수 있다.
	for _, peer := range p.peers {
		go func(to uint32) {
			resp, err := p.transport.SendPrepare(ctx, to, &pb.PrepareRequest{Ballot: ballot})
			prepareCh <- prepResult{resp, err}
		}(peer)
	}

	// 모든 노드의 응답을 기다린다.(채널에 들어오는 순서대로 순차적으로 처리한다.)
	// 에러 응답 (네트워크 장애, 노드 다운): 무시하고 다음으로 진행.
	// Ok=false 응답 (더 높은 ballot에 이미 약속): 역시 무시.
	// Ok=true 응답만 promises에 추가.
	var promises []*pb.PrepareResponse
	for range p.peers {
		res := <-prepareCh
		if res.err != nil {
			p.log.Debug("proposer: prepare failed", "slot", slot, "ballot", ballot, "error", res.err)
			continue
		}
		if res.resp.Ok {
			promises = append(promises, res.resp)
		}
	}

	// 과반수 미달 시 즉시 실패를 반환한다.
	if len(promises) < p.majority {
		return nil, fmt.Errorf("proposer: prepare failed, need %d promises, got %d", p.majority, len(promises))
	}

	// 자신이 제안한 값을 기본 값으로 설정한다.
	value := cmd
	// 가장 높은 ballot(제안 번호 + 노드 ID)을 추적하기 위해 변수를 선언한다.
	var highestBallot *pb.Ballot

	// 모든 promise의 AcceptedEntries를 순회하여, 현재 슬롯에 대해 이미 수락된 값 중 가장 높은 ballot을 찾는다.
	for _, promise := range promises {
		for _, entry := range promise.AcceptedEntries {
			// 현재 슬롯의 entry만 체크한다.
			if entry.Slot == slot {
				// 수락된 ballot이 아직 없거나, entry의 ballot이 더 높은 경우
				if highestBallot == nil || BallotGreaterThan(entry.Ballot, highestBallot) {
					highestBallot = entry.Ballot // 가장 높은 ballot(제안 번호 + 노드 ID)을 추적한다.
					value = entry.Command        // 자신의 값이 아니라, 이미 수락된 값을 채택한다.
					p.log.Info("proposer: adopted existing value", "slot", slot, "ballot", entry.Ballot, "key", entry.Command.Key, "value", entry.Command.Value)
				}
			}
		}
	}

	// 2 단계: Accept

	p.log.Info("proposer: start accept", "slot", slot, "value_op", value.Op, "value_key", value.Key)

	type acceptResult struct {
		resp *pb.AcceptResponse
		err  error
	}
	acceptCh := make(chan acceptResult, len(p.peers))

	// 각 노드에 대해 goroutine을 생성하여 병렬로 Accept 요청을 전송한다.
	for _, peer := range p.peers {
		go func(to uint32) {
			resp, err := p.transport.SendAccept(ctx, to, &pb.AcceptRequest{
				Slot:    slot,   // 현재 슬롯에 대해
				Ballot:  ballot, // Phase 1에서 사용한 것과 동일한 ballot
				Command: value,  // 수락된 값(또는 자신의 값)
			})
			acceptCh <- acceptResult{resp, err}
		}(peer)
	}

	acceptCount := 0
	for range p.peers {
		res := <-acceptCh
		if res.err != nil {
			continue
		}
		if res.resp.Ok {
			acceptCount++
		}
	}

	// 만약 다른 proposer가 더 높은 ballot으로 Prepare를 성공시켰다면, Accept 요청이 거절 될 수 있다!
	if acceptCount < p.majority {
		return nil, fmt.Errorf("proposer: accept failed, need %d accepts, got %d", p.majority, acceptCount)
	}

	p.log.Info("proposer: value chosen", "slot", slot, "value_op", value.Op, "value_key", value.Key)
	return value, nil
}
