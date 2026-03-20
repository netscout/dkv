package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"dkv/consensus"
	"dkv/storage/wal"
	"dkv/transport"
	pb "dkv/transport/proto/dkvpb"
)

const (
	HeartbeatInterval = 100 * time.Millisecond
	ElectionTimeout   = 500 * time.Millisecond
	ElectionJitter    = 300 * time.Millisecond
)

// 커밋 대기중인 클라이언트를 추적
type pendingProposal struct {
	done chan struct{} // 커밋 완료 시그널
	err  error
}

// 멀티 paxos를 지원하는 노드
// consensus.Consensus 인터페이스와 transport.MessageHandler 인터페이스를 구현한다.
type MultiPaxos struct {
	mu     sync.RWMutex
	nodeID uint32
	peers  []uint32

	acceptor *Acceptor
	proposer *Proposer
	rlog     *ReplicatedLog

	transport transport.Transport
	wal       *wal.WAL
	log       *slog.Logger

	// 리더 상태
	isLeader     bool
	leaderID     uint32
	leaderBallot *pb.Ballot

	// 채널
	commitCh chan *pb.LogEntry
	stopCh   chan struct{}
	stopOnce sync.Once // stopCh를 한번만 닫기 위해 사용

	// 커밋 대기중인 제안 목록
	pending map[uint64]*pendingProposal
	// pending 목록을 동기화하기 위해 사용
	pendingMu sync.Mutex

	// 전달 상태(deliverCommitted 함수 전체를 동기화하기 위해 사용)
	deliverMu     sync.Mutex
	lastDelivered uint64 // commitCh에 전송된 가장 높은 슬롯 번호

	// state machine에 적용된 가장 높은 슬롯 번호 -> 복구 과정에서 사용된다.
	lastApplied uint64

	// 마지막으로 리더의 하트비트를 수신한 시간
	lastHeartbeatRenewedRenewed time.Time

	// [v3 Issue #2 해결] lastHeartbeatRenewed와 lastLeaseRenewed가 별개의 필드여야 하는 이유:
	//
	// lastHeartbeatRenewed: 선거 타이머 억제용. 다음 3곳에서 갱신된다:
	//   (1) HandleHeartbeat -- 실제 리더 하트비트 수신 시
	//   (2) heartbeatLoop -- 리더 자신이 하트비트 전송 후 자기 타이머 리셋
	//   (3) tryBecomeLeader (Phase 2) -- 거부 응답에 LeaseActive=true가 있을 때만 선거 타이머 억제
	//       (LeaseActive=true는 리더가 살아있다는 직접 증거. stale ballot만으로는 억제하지 않음)
	// electionLoop에서 `elapsed > ElectionTimeout` 체크에 사용된다.
	//
	// lastLeaseRenewed: Acceptor의 leader lease 판단용. 다음 3곳에서 갱신된다:
	//   (1) HandleHeartbeat -- 실제 리더로부터 하트비트를 수신한 경우
	//   (2) tryBecomeLeader -- 리더로 당선된 직후 (자기 자신의 Acceptor를 보호하기 위해)
	//   (3) heartbeatLoop -- 리더가 하트비트 전송 후 자기 lease 갱신
	// HandlePrepare에서 `time.Since(lastLeaseRenewed) < ElectionTimeout` 체크에 사용된다.
	//
	// 핵심 차이: lastHeartbeatRenewed는 "리더에 대해 알게 된 시점" (거부 응답 포함)이고,
	// lastLeaseRenewed는 "실제 리더가 살아있음을 확인한 시점" (하트비트 + 리더 자신의 갱신만).
	// tryBecomeLeader에서 거부 응답을 받으면 lastHeartbeatRenewed는 갱신하여 선거 타이머를
	// 억제하지만, lastLeaseRenewed는 갱신하지 않는다. 거부 응답은 리더가
	// 살아있다는 직접적 증거가 아니므로, Acceptor의 lease를 연장하면 안 된다.
	// 단, 리더 자신은 tryBecomeLeader 당선 시와 heartbeatLoop에서 갱신한다.
	// 리더가 살아있는 한 자기 Acceptor도 challenger의 Prepare를 거부해야 하기 때문이다.
	lastLeaseRenewed time.Time

	// 선거 시도 횟수 카운터 (테스트 전용, Phase 2 suppression 효과 검증용).
	// tryBecomeLeader가 호출될 때마다 atomic으로 증가한다.
	electionAttempts atomic.Uint64
}

// 인터페이스 구현 체크
var _ consensus.Consensus = (*MultiPaxos)(nil)
var _ transport.MessageHandler = (*MultiPaxos)(nil)

func NewMultiPaxos(nodeID uint32, peers []uint32, w *wal.WAL, t transport.Transport, logger *slog.Logger) *MultiPaxos {
	mp := &MultiPaxos{
		nodeID:       nodeID,
		peers:        peers,
		acceptor:     NewAcceptor(nodeID, w, logger),
		proposer:     NewProposer(nodeID, peers, t, logger),
		rlog:         NewReplicatedLog(),
		wal:          w,
		transport:    t,
		log:          logger,
		leaderBallot: &pb.Ballot{},
		commitCh:     make(chan *pb.LogEntry, 256),
		stopCh:       make(chan struct{}),
		pending:      make(map[uint64]*pendingProposal),
	}

	return mp
}

// Consensus 인터페이스 구현--------------------------------------------------------------------

// 새로운 명령을 합의 과정에 제안하는 함수
// 명령이 커밋될 때까지 대기하며, 커밋되면 해당 명령이 처리된 슬롯의 인덱스 번호를 반환
func (mp *MultiPaxos) Propose(ctx context.Context, cmd *pb.Command) (uint64, error) {
	// 1. 리더 확인
	mp.mu.RLock()
	isLeader := mp.isLeader
	mp.mu.RUnlock()

	if !isLeader {
		return 0, fmt.Errorf("not leader (leader=%d)", mp.LeaderID())
	}

	// 2. 다음 슬롯 번호 확인
	slot := mp.rlog.NextSlot()

	// 3. 커밋까지 대기할 수 있도록 pendingProposal을 등록한다.
	pp := &pendingProposal{done: make(chan struct{})}
	mp.pendingMu.Lock()
	mp.pending[slot] = pp // 슬롯에 대한 pendingProposal을 등록한다. <- 슬롯이 commitCh에 전달되면 done 채널을 닫아서 커밋 완료를 알린다.
	mp.pendingMu.Unlock()

	defer func() { // 함수 종료시 성공이든 실패든 pending 목록에서 제거한다.
		mp.pendingMu.Lock()
		delete(mp.pending, slot)
		mp.pendingMu.Unlock()
	}()

	// 4. 현재 리더의 ballot 가져오기 -> 리더는 이미 Prepare 단계를 완료했으므로 Accept 단계만 실행한다.
	mp.mu.RLock()
	ballot := BallotClone(mp.leaderBallot)
	mp.mu.RUnlock()

	mp.log.Info("propose: accept 단계 시작",
		"slot", slot,
		"ballot", BallotString(ballot),
		"op", cmd.Op,
		"key", cmd.Key)

	acceptCount := 0
	majority := len(mp.peers)/2 + 1

	type acceptResult struct {
		ok  bool
		err error
	}
	ch := make(chan acceptResult, len(mp.peers)) // 채널의 버퍼 크기를 노드 수와 같게 설정하여 블로킹 없이 결과를 전송할 수 있도록 한다.

	// 5. 각 노드에 대해 goroutine을 생성하여 병렬로 Accept 요청을 전송한다.
	for _, peer := range mp.peers {
		go func(to uint32) {
			resp, err := mp.transport.SendAccept(ctx, to, &pb.AcceptRequest{
				Slot:    slot,
				Ballot:  ballot,
				Command: cmd,
			})
			if err != nil {
				ch <- acceptResult{false, err}
				return
			}
			if !resp.Ok { // Acceptor가 Ok: false와 자신의 PromisedBallot을 반환하면, 누군가 더 높은 ballot으로 Prepare를 성공시켰다는 신호이다.
				mp.mu.Lock()                                                 // 쓰기 잠금
				if BallotGreaterThan(resp.PromisedBallot, mp.leaderBallot) { // 더 높은 ballot을 가진 노드가 있으므로, 리더에서 물러나기
					mp.isLeader = false
					mp.log.Warn("propose: 리더십 상실",
						"higherBallot", BallotString(resp.PromisedBallot),
						"myBallot", BallotString(mp.leaderBallot))
				}
				mp.mu.Unlock()
			}
			ch <- acceptResult{resp.Ok, nil} // Acceptor가 Ok: true를 반환하면, 수락된 것으로 간주한다.
		}(peer)
	}

	// 6. 모든 노드의 응답을 기다린다.
	for range mp.peers {
		res := <-ch
		if res.ok { // Acceptor가 수락한 횟수를 카운트한다.
			acceptCount++
		}
	}

	mp.log.Debug("propose: accept 결과",
		"slot", slot,
		"accepts", acceptCount,
		"majority", majority)

	// 7. 과반수 미달 시 즉시 실패를 반환한다.
	if acceptCount < majority {
		return 0, fmt.Errorf("propose: accept failed, need %d accepts, got %d", majority, acceptCount)
	}

	// 8. 선택된(chosen) 값을 리더의 rlog에 저장하고 커밋으로 표시한다.
	entry := &pb.LogEntry{Index: slot, Ballot: ballot, Command: cmd}
	mp.rlog.Set(slot, entry)
	mp.rlog.MarkCommitted(slot)

	// 9. 연속적으로 커밋된 엔트리 목록을 전달(순서 보장을 위해) -> 이 함수의 끝에서 pp.done 채널이 닫히는 걸 대기한다.
	mp.deliverCommitted()

	// 10. 자신을 제외한 필로워 노드에게 커밋 정보를 전달한다.(fire-and-forget 팔로워가 받지 못해도 무시한다.)
	mp.log.Debug("propose: 커밋 브로드캐스트",
		"slot", slot,
		"key", cmd.Key)
	for _, peer := range mp.peers {
		if peer == mp.nodeID {
			continue
		}
		go func(to uint32) {
			// 팔로원의 HandleCommit이 커밋된 엔트리를 수신하고, 자신의 rlog에 저장하고 커밋으로 표시하게 된다.
			mp.transport.SendCommit(ctx, to, &pb.CommitRequest{
				Slot:    slot,
				Command: cmd,
			})
		}(peer)
	}

	// 두 가지 중에 먼저 발생하는 이벤트를 기다린다.
	select {
	case <-pp.done: // 전송한 로그 엔트리가 커밋 채널에 전달될 때까지 대기
		return slot, pp.err
	case <-ctx.Done(): // 컨텍스트 취소 시 즉시 실패를 반환한다.(commitCh 버퍼가 가득차거나, 이전 슬롯이 커밋되지 않아 순서 보장을 위해 deliverCommitted 에서 전달되지 않은 경우)
		return 0, ctx.Err()
	}
}

// 커밋된 LogEntry를 commitCh에 전달한다. -> commitCh를 수신하여 key-value store에 적용하는 고루틴에서 사용된다.
func (mp *MultiPaxos) deliverCommitted() {
	mp.deliverMu.Lock()
	defer mp.deliverMu.Unlock()

	for {
		// 다음으로 전송해야 할 슬롯 번호를 가져온다.
		next := mp.lastDelivered + 1
		// 슬롯 번호에 해당하는 엔트리가 커밋되지 않았다면, 전송을 중단한다.(순서 보장을 위해)
		if !mp.rlog.IsCommitted(next) {
			break
		}
		// 커밋은 되었지만 엔트리가 없다면, 전송을 중단한다.
		entry := mp.rlog.Get(next)
		if entry == nil {
			break
		}

		select {
		case mp.commitCh <- entry: // commitCh에 전달 가능한 경우
			mp.lastDelivered = next

			// pendingProposal이 커밋되었음을 알림
			mp.pendingMu.Lock()
			if pp, ok := mp.pending[next]; ok {
				close(pp.done) // pp.done 채널을 닫아서 Propose에서 대기하고 있는 고루틴을 깨운다.
			}
			mp.pendingMu.Unlock()
		default: // commitCh가 꽉 찼으므로, 다음 deliverCommitted 호출시 재시도 하도록 한다.
			mp.log.Warn("propose: commitCh 가득참, 건너뜀", "slot", next)
			return
		}
	}
}

// 커밋된 LogEntry를 수신하는 채널을 리턴한다.
func (mp *MultiPaxos) Committed() <-chan *pb.LogEntry {
	return mp.commitCh
}

func (mp *MultiPaxos) LeaderID() uint32 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.leaderID
}

func (mp *MultiPaxos) IsLeader() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return mp.isLeader
}

// 노드 복구시 사용, 이미 커밋된 엔트리가 다시 전달되지 않도록 상태 값을 업데이트한다.
func (mp *MultiPaxos) SetLastApplied(index uint64) {
	mp.mu.Lock()
	mp.lastApplied = index
	mp.mu.Unlock()

	// 복구 과정에서 로그 엔트리는 commitCh에 전달되지 않고 진행되므로, 전달된 가장 높은 슬롯 번호를 업데이트한다.
	mp.deliverMu.Lock()
	if index > mp.lastDelivered {
		mp.lastDelivered = index
	}
	mp.deliverMu.Unlock()
}

func (mp *MultiPaxos) Start() error {
	mp.log.Info("paxos: 시작",
		"peers", mp.peers,
		"electionTimeout", ElectionTimeout,
		"heartbeatInterval", HeartbeatInterval)
	go mp.electionLoop()  // 리더 부재시 리더 선출 루프를 시작한다.
	go mp.heartbeatLoop() // 리더의 하트비트 전송 루프를 시작한다.
	go mp.catchupLoop()   // 팔로워가 놓친 엔트리가 있는지 리더에게 요청하여 처리하는 루프를 시작한다.
	return nil
}

func (mp *MultiPaxos) Stop() {
	// Stop이 여러번 호출되더라도, stopCh를 한번만 닫는다.
	mp.stopOnce.Do(func() {
		close(mp.stopCh)
	})
}

// 놓친 엔트리가 존재하는지 주기적으로 리더에게 확인한다.
func (mp *MultiPaxos) catchupLoop() {
	ticker := time.NewTicker(1 * time.Second) // 백업 루프이므로, 하트비트(100ms)보다 느리게 설정한다.
	defer ticker.Stop()

	for {
		select {
		case <-mp.stopCh:
			return
		case <-ticker.C:
			mp.mu.RLock()
			isLeader := mp.isLeader
			leaderID := mp.leaderID
			mp.mu.RUnlock()

			if isLeader || leaderID == 0 { // 리더이면 catchup 할 필요가 없고, 리더가 없으면 요청할 대상이 없다.
				continue
			}

			mp.deliverMu.Lock()
			afterSlot := mp.lastDelivered // 마지막으로 전달 받은 슬롯 번호를 가져온다.
			mp.deliverMu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)  // 타임아웃은 2초로 설정한다.
			resp, err := mp.transport.SendCatchup(ctx, leaderID, &pb.CatchupRequest{ // 리더의 HandleCatchup이 rlog.EntriesAfter(afterSlot) 으로 응답한다.
				AfterSlot: afterSlot,
			})
			cancel()
			if err != nil {
				mp.log.Info("catchup: 리더에게 요청 실패",
					"leader", leaderID,
					"afterSlot", afterSlot,
					"err", err)
				continue
			}

			if len(resp.Entries) > 0 {
				mp.log.Info("catchup: 놓친 엔트리 수신",
					"leader", leaderID,
					"afterSlot", afterSlot,
					"received", len(resp.Entries))
			}

			// 전달 받은 모든 엔트리를 rlog에 저장하고 커밋으로 표시한다.
			for _, entry := range resp.Entries {
				mp.rlog.Set(entry.Index, entry)
				mp.rlog.MarkCommitted(entry.Index)
			}
			// commitCh에 커밋된 엔트리를 전달하여 key-value store에 적용하도록 한다.
			mp.deliverCommitted()
		}
	}
}

// transport.MessageHandler 인터페이스 구현--------------------------------------------------------------------

func (mp *MultiPaxos) HandlePrepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	// [v3 Issue #4 해결] 2계층 Prepare 게이트 구조:
	//
	// Layer 1 (여기): Leader Lease 게이트 -- MultiPaxos 레벨.
	//   최근에 리더로부터 하트비트를 받은 경우, ballot 값에 관계없이 다른 노드의 Prepare를 거부한다.
	//   이는 liveness 최적화이며, safety에 영향을 주지 않는다 (Prepare 거부는 항상 안전하다).
	//   Acceptor의 promisedBallot은 변경되지 않는다.
	//
	// Layer 2 (acceptor.HandlePrepare): Promise/Ballot 게이트 -- Acceptor 레벨.
	//   표준 Paxos promise 로직. 이미 더 높은 ballot에 promise한 경우 거부한다.
	//   promisedBallot을 갱신하고, 수락된 값을 응답에 포함한다.
	//
	// 두 게이트를 모두 통과해야 Prepare가 성공한다. Layer 1은 Layer 2의 상태를 변경하지 않으므로,
	// 향후 Acceptor 로직을 수정할 때 Layer 1의 존재를 고려해야 한다.

	// Leader Lease: 최근에 리더로부터 하트비트를 받은 경우, 다른 노드의 Prepare를 거부한다.
	// 자신이 보낸 Prepare는 lease 체크를 건너뛴다 (자신의 nodeId와 일치하는 경우).
	mp.log.Debug("prepare: Prepare 수신",
		"challenger", req.Ballot.NodeId,
		"challengerBallot", BallotString(req.Ballot))
	mp.mu.RLock()
	leaderLeaseActive := time.Since(mp.lastLeaseRenewed) < ElectionTimeout
	lastLeaderHB := mp.lastLeaseRenewed // lease 나이 계산용 (RLock 안에서 캡처)
	currentLeaderBallot := BallotClone(mp.leaderBallot)
	mp.mu.RUnlock()

	if leaderLeaseActive && req.Ballot.NodeId != mp.nodeID {
		mp.log.Info("prepare: leader lease 활성 -> 거부",
			"challenger", req.Ballot.NodeId,
			"challengerBallot", BallotString(req.Ballot),
			"leaderBallot", BallotString(currentLeaderBallot),
			"leaseAge", time.Since(lastLeaderHB).Round(time.Millisecond))
		// [v3 Issue #5 해결] 거부 응답에 currentLeaderBallot을 반환하는 이유:
		// Lease가 활성인 동안 Acceptor의 promisedBallot은 무관하다 -- lease 자체가 거부 사유이다.
		// currentLeaderBallot이 promisedBallot보다 낮을 수 있지만 (이전 challenger가 Acceptor의
		// promisedBallot을 갱신한 경우), 이는 문제가 되지 않는다:
		//   (a) Lease가 활성인 동안 challenger는 어차피 Acceptor에 도달할 수 없다.
		//   (b) Lease 만료 후 challenger가 재시도하면 Acceptor의 실제 promisedBallot을 받게 된다.
		//   (c) Phase 2의 억제 메커니즘에서 challenger는 이 값으로 leaderBallot을 설정하는데,
		//       stale 값이어도 억제 기간(1500ms) 동안 하트비트가 도착하여 올바른 값으로 교정된다.
		return &pb.PrepareResponse{
			Ok:             false,
			PromisedBallot: currentLeaderBallot,
			LeaseActive:    true, // leader lease 활성 상태에서 거부 -> 리더가 살아있다는 직접 증거
		}, nil
	}

	mp.log.Debug("prepare: leader lease 비활성, acceptor에게 위임",
		"challenger", req.Ballot.NodeId,
		"challengerBallot", BallotString(req.Ballot))
	return mp.acceptor.HandlePrepare(ctx, req)
}

func (mp *MultiPaxos) HandleAccept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	return mp.acceptor.HandleAccept(ctx, req)
}

// 팔로워가 커밋된 엔트리를 수신하면, rlog에 저장하고 커밋으로 표시한다.
func (mp *MultiPaxos) HandleCommit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	mp.log.Debug("commit: 커밋 수신",
		"slot", req.Slot,
		"key", req.Command.Key,
		"op", req.Command.Op)
	entry := &pb.LogEntry{Index: req.Slot, Command: req.Command} // Ballot은 전달되지 않는다. 팔로워는 리더의 ballot을 알 필요가 없다.
	mp.rlog.Set(req.Slot, entry)
	mp.rlog.MarkCommitted(req.Slot)
	mp.deliverCommitted()
	return &pb.CommitResponse{}, nil
}

// 리더가 팔로워의 catchup 요청을 처리한다.
func (mp *MultiPaxos) HandleCatchup(ctx context.Context, req *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	entries := mp.rlog.EntriesAfter(req.AfterSlot)
	mp.log.Debug("catchup: 요청 처리",
		"afterSlot", req.AfterSlot,
		"entriesReturned", len(entries))
	return &pb.CatchupResponse{Entries: entries}, nil
}

// 리더의 하트비트 수신 시 처리
func (mp *MultiPaxos) HandleHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// 로그 캡처 변수 (lock 안에서 값을 캡처하고, lock 해제 후 로깅한다)
	type heartbeatLogType int
	const (
		hbLogNone      heartbeatLogType = iota
		hbLogNewLeader                  // 새 리더 감지 (INFO)
		hbLogRoutine                    // 동일 리더 하트비트 (DEBUG)
		hbLogStale                      // stale 하트비트 (DEBUG)
	)
	logType := hbLogNone
	var logLeader, logPrevLeader uint32
	var logBallot, logCurrentBallot string
	var logCommittedUpTo uint64

	mp.mu.Lock()

	prevLeaderID := mp.leaderID

	// 리더 상태 갱신(lastHeartbeatRenewed를 갱신하기 위해 같은 ballot도 허용한다.)
	if BallotGreaterOrEqual(req.Ballot, mp.leaderBallot) {
		mp.leaderID = req.LeaderId
		mp.leaderBallot = BallotClone(req.Ballot)
		mp.isLeader = false // 팔로워만 하트비트를 수신하므로!
		mp.lastHeartbeatRenewedRenewed = time.Now()
		// Leader Lease: Acceptor가 Prepare 거부 판단에 사용하는 하트비트 시간 갱신.
		// lastHeartbeatRenewed와 달리, 이 필드는 실제 리더 하트비트와 리더 자신의 갱신에서만 갱신된다.
		// (tryBecomeLeader의 거부 응답에서는 갱신하지 않음 -- 필드 주석 참조)
		mp.lastLeaseRenewed = time.Now()

		// 로그 값 캡처 (lock 밖에서 로깅하기 위함)
		if prevLeaderID != req.LeaderId {
			logType = hbLogNewLeader
			logLeader = req.LeaderId
			logBallot = BallotString(req.Ballot)
			logPrevLeader = prevLeaderID
		} else if mp.log.Enabled(context.Background(), slog.LevelDebug) {
			// Enabled()는 내부적으로 atomic 비교만 수행하므로 lock 안에서 호출해도 안전하다.
			logType = hbLogRoutine
			logLeader = req.LeaderId
			logBallot = BallotString(req.Ballot)
			logCommittedUpTo = req.CommittedUpTo
		}
	} else {
		// 이전 리더의 stale 하트비트 무시 (리더 상태 갱신하지 않음)
		logType = hbLogStale
		logLeader = req.LeaderId
		logBallot = BallotString(req.Ballot)
		logCurrentBallot = BallotString(mp.leaderBallot)
	}

	// committedUpTo 처리: 기존 코드의 동작을 보존한다 (fall-through).
	// 원래 코드에는 else 브랜치가 없어 ballot 비교 결과와 무관하게 항상 실행되었다.
	// 의도적 설계인지 우연인지 불분명하지만, 동작 변경을 방지하기 위해 유지한다.
	myCommittedUpTo := mp.rlog.CommittedUpTo()
	newCommits := uint64(0)
	for slot := myCommittedUpTo + 1; slot <= req.CommittedUpTo; slot++ {
		if mp.rlog.Get(slot) != nil { // 자신의 rlog에 있는 엔트리를 커밋 처리
			mp.rlog.MarkCommitted(slot)
			newCommits++
		} else {
			mp.log.Debug("heartbeat: 놓친 엔트리 발견, catchup 필요",
				"missingSlot", slot,
				"leaderCommittedUpTo", req.CommittedUpTo)
			// 놓친 엔트리가 존재
			// catchupLoop에서 리더에게 요청하여 처리
			break
		}
	}
	if newCommits > 0 {
		mp.log.Debug("heartbeat: 하트비트로 커밋 처리",
			"newCommits", newCommits,
			"myCommittedUpTo", myCommittedUpTo,
			"leaderCommittedUpTo", req.CommittedUpTo)
	}

	mp.mu.Unlock()

	// lock 해제 후 로깅 (모든 브랜치에서 일관되게 lock 밖에서 로깅)
	switch logType {
	case hbLogNewLeader:
		mp.log.Info("heartbeat: 새 리더 감지",
			"leader", logLeader,
			"ballot", logBallot,
			"prevLeader", logPrevLeader)
	case hbLogRoutine:
		mp.log.Debug("heartbeat: 수신",
			"leader", logLeader,
			"ballot", logBallot,
			"committedUpTo", logCommittedUpTo)
	case hbLogStale:
		mp.log.Debug("heartbeat: stale 하트비트 무시",
			"from", logLeader,
			"reqBallot", logBallot,
			"currentBallot", logCurrentBallot)
	}

	mp.deliverCommitted()

	return &pb.HeartbeatResponse{Success: true}, nil
}

// 리더 부재시 리더 선출 루프에서 호출된다.
func (mp *MultiPaxos) tryBecomeLeader() error {
	mp.electionAttempts.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mp.mu.Lock()
	oldNumber := mp.proposer.ballot.Number
	mp.proposer.ballot.Number++ // 새로운 ballot을 생성한다.
	ballot := BallotClone(mp.proposer.ballot)
	mp.mu.Unlock()

	mp.log.Info("election: 🟢 prepare 시작",
		"ballot", BallotString(ballot),
		"prevNumber", oldNumber)

	// 모두에게 Prepare 요청을 전송하고
	type prepResult struct {
		resp *pb.PrepareResponse
		err  error
	}
	ch := make(chan prepResult, len(mp.peers))
	for _, peer := range mp.peers {
		go func(to uint32) {
			resp, err := mp.transport.SendPrepare(ctx, to, &pb.PrepareRequest{Ballot: ballot})
			if err != nil {
				mp.log.Info("election: prepare 전송 실패", "peer", to, "ballot", BallotString(ballot), "err", err)
			} else if !resp.Ok {
				mp.log.Info("election: prepare 거부됨", "peer", to, "ballot", BallotString(ballot), "promisedBallot", BallotString(resp.PromisedBallot))
			} else {
				mp.log.Info("election: prepare 약속 받음", "peer", to, "ballot", BallotString(ballot))
			}
			ch <- prepResult{resp, err}
		}(peer)
	}

	// 모든 노드의 응답을 기다린다.
	var promises []*pb.PrepareResponse
	var maxRejectedNumber uint64 // fast-forward용: 거부 응답 중 가장 높은 ballot number
	leaseActiveDetected := false // suppression용: LeaseActive=true인 거부가 하나라도 있는지
	for range mp.peers {
		res := <-ch
		if res.err == nil && res.resp.Ok {
			promises = append(promises, res.resp)
		} else if res.err == nil && !res.resp.Ok {
			// 거부 응답에서 fast-forward 및 suppression 정보 수집
			if res.resp.PromisedBallot != nil && res.resp.PromisedBallot.Number > maxRejectedNumber {
				maxRejectedNumber = res.resp.PromisedBallot.Number
			}
			// LeaseActive=true: 거부 노드가 리더로부터 최근 하트비트를 받았음 -> 리더 생존 직접 증거
			if res.resp.LeaseActive {
				leaseActiveDetected = true
			}
		}
	}

	// 과반수 미달 시 즉시 실패를 반환하고 다음 타이머에서 재시도한다.
	majority := len(mp.peers)/2 + 1
	// 결과를 조건 변수로 미리 계산 (slog 호출 안에서 익명 함수를 사용하지 않는다)
	result := "실패"
	if len(promises) >= majority {
		result = "성공"
	}
	mp.log.Info("election: prepare 결과",
		"promises", len(promises),
		"majority", majority,
		"ballot", BallotString(ballot),
		"result", result)
	if len(promises) < majority {
		// [레이스 윈도우 주석]
		// 이 시점(응답 수집 완료)과 아래 mp.mu.Lock() 사이에 짧은 윈도우가 존재한다.
		// 이 윈도우 동안 electionLoop가 lastHeartbeatRenewed를 읽어 또 한 번의 tryBecomeLeader를
		// 호출할 수 있지만, 이는 의도적으로 허용된다.
		// 최악의 경우: 불필요한 선거 시도 1회 추가 (safety에 영향 없음).
		// 하트비트가 이 윈도우에서 lastHeartbeatRenewed를 갱신하더라도, 아래에서 time.Now()로
		// 덮어쓰는 것은 양쪽 모두 최신 타임스탬프이므로 benign하다.
		// 로그용 변수 캡처 (HandleHeartbeat와 동일한 패턴: lock 안에서 캡처, lock 밖에서 로깅)
		var fastForwardFrom uint64
		fastForwarded := false
		suppressed := false

		mp.mu.Lock()
		// [Fast-Forward] 거부 응답에서 가장 높은 ballot number로 점프
		// 다음 tryBecomeLeader 호출에서 Number++가 되므로 maxRejectedNumber + 1이 된다
		if maxRejectedNumber > mp.proposer.ballot.Number {
			fastForwardFrom = mp.proposer.ballot.Number
			fastForwarded = true
			mp.proposer.ballot.Number = maxRejectedNumber
		}
		// [Suppression] LeaseActive=true인 거부가 있을 때만 선거 타이머 억제
		// LeaseActive=true는 "거부 노드가 리더로부터 최근 하트비트를 받았음"을 의미하므로,
		// 리더가 살아있다는 직접 증거이다.
		// 주의: leaderID/leaderBallot은 설정하지 않는다 (safety review #3).
		// lastLeaseRenewed도 갱신하지 않는다 (Acceptor lease 연장 방지).
		//
		// [Suppression 효과 한계 문서화]
		// Suppression은 lastHeartbeatRenewed = time.Now()를 설정하여 선거 타이머를 리셋한다.
		// 그러나 electionLoop의 타이머가 ElectionTimeout + jitter (500~800ms) 후에 발동하므로,
		// 다음 타이머 발동 시 elapsed > ElectionTimeout이 다시 true가 되어 선거가 재시작된다.
		// 따라서 suppression은 선거를 완전히 방지하는 것이 아니라, 1 타이머 주기(~300ms jitter)만큼
		// 지연시키는 효과만 있다. 리더십 탈취 방지의 실질적 보호는 leader lease가 담당한다.
		// Suppression의 가치는: (1) 리더가 살아있을 때 불필요한 RPC를 약간 줄여주는 최적화,
		// (2) 향후 더 강력한 억제 메커니즘(예: forward-projected timestamp)으로 발전시킬 기반.
		if leaseActiveDetected {
			mp.lastHeartbeatRenewedRenewed = time.Now()
			suppressed = true
		}
		mp.mu.Unlock()

		// lock 해제 후 로깅 (HandleHeartbeat와 동일한 패턴)
		if fastForwarded {
			mp.log.Info("election: ballot fast-forward",
				"from", fastForwardFrom,
				"to", maxRejectedNumber)
		}
		if suppressed {
			mp.log.Info("election: 리더 생존 감지, 선거 타이머 억제",
				"maxRejectedBallot", maxRejectedNumber)
		}
		return fmt.Errorf("🔴election failed: %d/%d promises", len(promises), majority)
	}

	// 이전 리더가 Accept를 과반수에게 보내고 MarkCommitted를 했지만 커밋 브로드캐스드를 보내기 전에 죽었다면, 리더가 된 자신이 다시 제안하여 커밋 처리한다.
	// TODO: 같은 슬롯에 대해 다른 값이 보고 되는 경우, 가장 높은 ballot을 선택해야 하지만 현재는 그런 처리가 없음.
	for _, promise := range promises { // 과반수에게 약속을 받은 경우
		for _, accepted := range promise.AcceptedEntries { // 각 노드의 AcceptedEntries를 순회하여, 커밋되지 않은 엔트리를 찾는다.
			if !mp.rlog.IsCommitted(accepted.Slot) { // 커밋되지 않은 엔트리를 찾았다면,
				mp.log.Info("election: 미커밋 엔트리 재제안",
					"slot", accepted.Slot,
					"ballot", BallotString(ballot),
					"key", accepted.Command.Key)
				entry := &pb.LogEntry{Index: accepted.Slot, Ballot: ballot, Command: accepted.Command}
				mp.rlog.Set(accepted.Slot, entry) // Prepare 단계에서 수락된 값이므로 rlog에 저장해도 무방하다.

				// 모든 노드에게 Accept 요청 전송하고, 응답을 기다린다.
				accCount := 0
				accCh := make(chan bool, len(mp.peers))
				for _, peer := range mp.peers {
					go func(to uint32) {
						resp, err := mp.transport.SendAccept(ctx, to, &pb.AcceptRequest{
							Slot: accepted.Slot, Ballot: ballot, Command: accepted.Command,
						})
						accCh <- (err == nil && resp.Ok)
					}(peer)
				}
				for range mp.peers {
					if <-accCh {
						accCount++
					}
				}

				// 과반수 이상의 노드가 수락한 경우, 커밋 처리한다.
				if accCount >= majority {
					mp.rlog.MarkCommitted(accepted.Slot)
				}
				mp.log.Debug("election: 재제안 accept 결과",
					"slot", accepted.Slot,
					"accepts", accCount,
					"majority", majority,
					"committed", accCount >= majority)
			}
		}
	}
	mp.deliverCommitted()

	// 자신을 리더로 설정한다.
	mp.mu.Lock()
	mp.isLeader = true
	mp.leaderID = mp.nodeID
	mp.leaderBallot = ballot
	mp.lastHeartbeatRenewedRenewed = time.Now()
	// [v4 FIX] 리더 자신의 Acceptor도 challenger의 Prepare를 거부해야 한다.
	// 리더는 자신에게 하트비트를 보내지 않으므로 HandleHeartbeat를 통해서는
	// lastLeaseRenewed가 갱신되지 않는다. 여기서 명시적으로 설정한다.
	mp.lastLeaseRenewed = time.Now()
	mp.mu.Unlock()

	mp.log.Info("election: 🎉 리더 당선",
		"ballot", BallotString(ballot),
		"committedUpTo", mp.rlog.CommittedUpTo())
	return nil
}

func (mp *MultiPaxos) electionLoop() {
	// 모든 노드가 동시에 선출을 시작하지 않도록 초기 랜덤 지연 시간을 적용한다.
	timer := time.NewTimer(ElectionTimeout + randomJitter())
	defer timer.Stop()

	for {
		select {
		case <-mp.stopCh:
			return
		case <-timer.C:
			mp.mu.RLock()
			isLeader := mp.isLeader
			elapsed := time.Since(mp.lastHeartbeatRenewedRenewed)
			mp.mu.RUnlock()

			if isLeader {
				// 리더는 선거를 시작하지 않는다 (DEBUG 레벨로만 출력)
				mp.log.Debug("election: 타이머 발동, 리더이므로 건너뜀")
			} else if elapsed <= ElectionTimeout {
				// 최근에 하트비트를 수신했으므로 선거를 건너뛴다
				mp.log.Debug("election: 타이머 발동, 하트비트 수신으로 건너뜀",
					"elapsed", elapsed.Round(time.Millisecond),
					"threshold", ElectionTimeout)
			} else {
				// 하트비트 타임아웃 초과, 선거 시작
				mp.log.Info("election: 타이머 발동, 선거 시작",
					"elapsed", elapsed.Round(time.Millisecond),
					"threshold", ElectionTimeout)
				if err := mp.tryBecomeLeader(); err != nil {
					mp.log.Info("election: 실패", "err", err)
				}
			}

			// 다음 리더 선출 시간을 설정한다.
			timer.Reset(ElectionTimeout + randomJitter())
		}
	}
}

// 랜덤한 jitter(지연시간) 0~ElectionJitter 사이의 값을 반환한다.
func randomJitter() time.Duration {
	return time.Duration(rand.Int63n(int64(ElectionJitter)))
}

// 리더가 하트비트를 전송하는 루프
func (mp *MultiPaxos) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval) // 하트비트 전송 주기를 설정한다.
	defer ticker.Stop()

	for {
		select {
		case <-mp.stopCh:
			return
		case <-ticker.C:
			mp.mu.RLock()
			isLeader := mp.isLeader
			ballot := BallotClone(mp.leaderBallot)
			mp.mu.RUnlock()

			// 리더가 아니면, 하트비트를 전송하지 않고 다음 타이머에서 재시도한다.
			if !isLeader {
				continue
			}

			// 리더가 하트비트와 함께 "나는 슬롯 X까지 커밋했다"고 전송한다.
			committedUpTo := mp.rlog.CommittedUpTo()
			for _, peer := range mp.peers {
				if peer == mp.nodeID { // 자신에게는 하트비트를 전송하지 않는다.
					continue
				}
				go func(to uint32) {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					defer cancel()
					_, err := mp.transport.SendHeartbeat(ctx, to, &pb.HeartbeatRequest{
						LeaderId:      mp.nodeID,
						Ballot:        ballot,
						CommittedUpTo: committedUpTo,
					})
					if err != nil {
						mp.log.Debug("heartbeat: 전송 실패",
							"peer", to,
							"err", err)
					}
				}(peer)
			}

			mp.mu.Lock()
			mp.lastHeartbeatRenewedRenewed = time.Now() // 하트비트를 전송했으므로, 하트비트 타임아웃 리셋
			// [v4 FIX] 리더 자신의 leader lease를 갱신한다.
			// 리더는 자신에게 하트비트를 보내지 않으므로, heartbeatLoop에서 직접 갱신해야 한다.
			// 이것이 없으면 tryBecomeLeader에서 설정한 lease가 ElectionTimeout(500ms) 후 만료되어
			// challenger가 리더의 Acceptor에서 promise를 받을 수 있다.
			mp.lastLeaseRenewed = time.Now()
			mp.mu.Unlock()
		}
	}
}

// GetAcceptor exposes the acceptor for WAL recovery.
func (mp *MultiPaxos) GetAcceptor() *Acceptor {
	return mp.acceptor
}

// ElectionAttempts는 이 노드가 시도한 선거 횟수를 반환한다. 테스트 전용.
func (mp *MultiPaxos) ElectionAttempts() uint64 {
	return mp.electionAttempts.Load()
}
