package paxos

import (
	"fmt"

	pb "dkv/transport/proto/dkvpb"
)

/*
투표권(Ballot)은 제안 번호(Number)와 노드 ID(NodeId)의 쌍으로 이루어져 있다.
*/

// 각 노드의 투표권(Ballot)의 제안 번호(Number)를 비교하여 a > b 이면 true, 아니면 false
// 제안 번호가 같으면 노드 ID가 더 높은 쪽이 우선권을 가짐
func BallotGreaterThan(a, b *pb.Ballot) bool {
	if a.Number != b.Number {
		return a.Number > b.Number
	}
	return a.NodeId > b.NodeId
}

// 각 노드의 투표권(Ballot)의 제안 번호(Number)와 노드 ID가 모두 같으면 true, 아니면 false
func BallotEqual(a, b *pb.Ballot) bool {
	return a.Number == b.Number && a.NodeId == b.NodeId
}

// 각 노드의 투표권(Ballot)의 제안 번호(Number)와 노드 ID가 모두 같거나, a > b 이면 true, 아니면 false
func BallotGreaterOrEqual(a, b *pb.Ballot) bool {
	return BallotEqual(a, b) || BallotGreaterThan(a, b)
}

func BallotIsZero(b *pb.Ballot) bool {
	return b == nil || (b.Number == 0 && b.NodeId == 0)
}

// ballot을 사람이 읽기 쉬운 형식으로 변환한다. 예: "N25.1" (Number=25, NodeId=1)
// slog의 구조화된 로깅에서 ballot 필드를 출력할 때 사용한다.
func BallotString(b *pb.Ballot) string {
	if b == nil {
		return "N0.0"
	}
	return fmt.Sprintf("N%d.%d", b.Number, b.NodeId)
}

// 여러 곳에서 동일한 Ballot을 사용할 때, 원본이 변경되지 않도록 복사본을 만든다.
func BallotClone(b *pb.Ballot) *pb.Ballot {
	if b == nil {
		return &pb.Ballot{}
	}
	return &pb.Ballot{
		Number: b.Number,
		NodeId: b.NodeId,
	}
}
