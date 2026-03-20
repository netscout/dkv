/*
Single Decree Paxos 테스트

"Single Decree"는 모든 제안이 동일한 슬롯(여기서는 슬롯 0번)을 대상으로 하는 경우를 말한다.
*/

package paxos

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	"dkv/transport"
	pb "dkv/transport/proto/dkvpb"
)

// 단일 노드 테스트를 위한 구조체
type singleNode struct {
	id       uint32
	acceptor *Acceptor
	proposer *Proposer
}

/*
transport.MessageHandler 인터페이스를 구현하여, singleNode를 InMemoryTransport에 등록할 수 있도록 한다.
*/

func (n *singleNode) HandlePrepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	return n.acceptor.HandlePrepare(ctx, req)
}

func (n *singleNode) HandleAccept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	return n.acceptor.HandleAccept(ctx, req)
}

/*
Commit, Catchup, Heartbeat는 멀티 paxos에서 필요하므로 여기서는 빈(no-op) 응답을 반환한다.
*/

func (n *singleNode) HandleCommit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	return &pb.CommitResponse{}, nil
}

func (n *singleNode) HandleCatchup(ctx context.Context, req *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	return &pb.CatchupResponse{}, nil
}

func (n *singleNode) HandleHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Success: true}, nil
}

// setupCluster 함수는 3개의 노드로 구성된 클러스터를 생성하고, InMemoryTransport에 등록한다.
func setupCluster(t *testing.T) ([]*singleNode, *transport.InMemoryTransport) {
	t.Helper()
	// 테스트 로그가 표준 출력에 나오도록 설정
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	transport := transport.NewInMemoryTransport()
	peers := []uint32{1, 2, 3}

	// 3개의 노드를 생성하고, InMemoryTransport에 등록한다.
	nodes := make([]*singleNode, len(peers))
	for i, id := range peers {
		n := &singleNode{
			id:       id,
			acceptor: NewAcceptor(id, nil, logger), // 테스트에서 wal 파일은 사용하지 않는다.
			proposer: NewProposer(id, peers, transport, logger),
		}
		nodes[i] = n
		transport.Register(id, n)
	}
	return nodes, transport
}

// 하나의 노드가 하나의 값을 제안하는 테스트
func TestSingleDecree_OneProposer(t *testing.T) {
	nodes, _ := setupCluster(t)
	ctx := context.Background()

	cmd := &pb.Command{Op: "PUT", Key: "x", Value: "42"}
	// 1번 노드가 0번 슬롯에 "x" 값을 제안한다.
	// 내부 로직에 따라, 3개의 노드에 제안 Prepare -> 3개 노드의 Accept 과정을 거친다.
	chosen, err := nodes[0].proposer.Propose(ctx, 0, cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if chosen.Key != "x" || chosen.Value != "42" {
		t.Fatalf("Chosen value mismatch: got %v, want %v", chosen, "42")
	}
}

// 3개의 노드가 동일한 슬롯에 대해 각각 다른 값을 제안하는 테스트
func TestSingleDecree_ConcurrentProposers(t *testing.T) {
	nodes, _ := setupCluster(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	results := make([]*pb.Command, len(nodes))
	errors := make([]error, len(nodes))

	// 3개의 노드가 각각 다른 값을 동시에 제안한다.
	for i := range nodes {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := &pb.Command{
				Op:    "PUT",
				Key:   "x",
				Value: fmt.Sprintf("A%d", idx),
			}
			results[idx], errors[idx] = nodes[idx].proposer.Propose(ctx, 0, cmd)
		}(i)
	}
	wg.Wait()

	// 최소 1개는 반드시 성공해야 한다
	var chosen *pb.Command
	for i, err := range errors {
		if err == nil {
			if chosen == nil {
				chosen = results[i]
			}
			// 성공한 제안은 반드시 동일한 값에 대해 합의해야 한다.
			if results[i].Value != chosen.Value {
				t.Fatalf("disagreement: node %d chose %q, another chose %q", i, results[i].Value, chosen.Value)
			}
		}
	}
	if chosen == nil {
		t.Fatalf("no value chosen")
	}
	t.Logf("chosen value: %q", chosen.Value)
}

// 하나의 노드가 중단된 경우, 나머지 2개의 노드가 합의를 이루는 테스트
func TestSingleDecree_OneNodeDown(t *testing.T) {
	nodes, transport := setupCluster(t)
	ctx := context.Background()

	// 3번 노드를 중단시킨다.
	transport.Disconnect(3)

	cmd := &pb.Command{Op: "PUT", Key: "y", Value: "99"}
	chosen, err := nodes[0].proposer.Propose(ctx, 0, cmd)
	if err != nil {
		t.Fatalf("should succeed with 2/3 majority: %v", err)
	}
	if chosen.Key != "y" || chosen.Value != "99" {
		t.Fatalf("chosen value mismatch: got %v, want %v", chosen, "99")
	}
}

// 두 개의 노드가 중단된 경우, 나머지 1개의 노드가 합의에 실패하는 테스트
func TestSingleDecree_TwoNodesDown(t *testing.T) {
	nodes, transport := setupCluster(t)
	ctx := context.Background()

	// 2, 3번 노드를 중단시킨다.
	transport.Disconnect(2)
	transport.Disconnect(3)

	cmd := &pb.Command{Op: "PUT", Key: "z", Value: "fail"}
	_, err := nodes[0].proposer.Propose(ctx, 0, cmd)
	if err == nil {
		t.Fatalf("should fail with only 1/3 nodes")
	}
}
