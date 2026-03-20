package paxos

import (
	"testing"

	pb "dkv/transport/proto/dkvpb"
)

// TestBallotGreaterThan_EqualNumberHigherNodeId: 제안번호가 같을 때 더 높은 노드 ID가 승리함을 검증한다.
func TestBallotGreaterThan_EqualNumberHigherNodeId(t *testing.T) {
	// 제안번호가 같을 때, 더 높은 노드 ID가 승리한다.
	a := &pb.Ballot{Number: 15, NodeId: 1}
	b := &pb.Ballot{Number: 15, NodeId: 3}

	// {15, 1}은 {15, 3}보다 크지 않다. nodeId 1 < 3이기 때문이다.
	if BallotGreaterThan(a, b) {
		t.Errorf("BallotGreaterThan({15,1}, {15,3}) = true, want false")
	}

	// {15, 3}은 {15, 1}보다 크다. nodeId 3 > 1이기 때문이다.
	if !BallotGreaterThan(b, a) {
		t.Errorf("BallotGreaterThan({15,3}, {15,1}) = false, want true")
	}
}

// TestBallotGreaterThan_HigherNumberWins: 제안번호가 더 높은 쪽이 노드 ID에 관계없이 항상 승리함을 검증한다.
func TestBallotGreaterThan_HigherNumberWins(t *testing.T) {
	a := &pb.Ballot{Number: 16, NodeId: 1}
	b := &pb.Ballot{Number: 15, NodeId: 3}

	// 제안번호가 더 높으면 노드 ID에 관계없이 항상 승리한다.
	if !BallotGreaterThan(a, b) {
		t.Errorf("BallotGreaterThan({16,1}, {15,3}) = false, want true")
	}
}

// Table-Driven Test 형식으로 투표권의 순서 비교 테스트 구현
func TestBallotOrdering(t *testing.T) {
	tests := []struct { // 익명 구조체 슬라이스
		name string     // 테스트 이름
		a, b *pb.Ballot // 투표권
		want bool       // 예상 결과
	}{
		{"높은 제안번호가 승리", &pb.Ballot{Number: 2, NodeId: 1}, &pb.Ballot{Number: 1, NodeId: 3}, true},
		{"같은 제안번호, 더 높은 노드 ID가 승리", &pb.Ballot{Number: 1, NodeId: 3}, &pb.Ballot{Number: 1, NodeId: 2}, true},
		{"같은 제안번호, 같은 노드 ID", &pb.Ballot{Number: 1, NodeId: 1}, &pb.Ballot{Number: 1, NodeId: 1}, false},
		{"더 낮은 제안번호가 패배", &pb.Ballot{Number: 1, NodeId: 1}, &pb.Ballot{Number: 2, NodeId: 1}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { // 각 테스트 케이스를 독립적인 서브테스트로 실행
			got := BallotGreaterThan(test.a, test.b)
			if got != test.want {
				// FatalF는 실패 기록 후 테스트를 중단하므로 ErrorF를 사용하여 계속 테스트 진행
				t.Errorf("BallotGreaterThan(%v, %v) = %v, 결과가 예상과 다름: 예상 %v", test.a, test.b, got, test.want)
			}
		})
	}
}
