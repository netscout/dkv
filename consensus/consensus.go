package consensus

import (
	"context"
	pb "dkv/transport/proto/dkvpb"
)

// 다른 패키지에서 사용할 수 있도록 protobuf 타입 별칭 정의
// type Command = pb.Command
// type LogEntry = pb.LogEntry
// type Ballot = pb.Ballot

type Consensus interface {
	// 새로운 명령을 합의 과정에 제안하는 함수
	// 명령이 커밋될 때까지 대기하며, 커밋되면 해당 명령이 처리된 슬롯의 인덱스 번호를 반환
	Propose(ctx context.Context, command *pb.Command) (uint64, error)

	// 리턴되는 LogEntry 채널은 커밋된 순서대로 로그 항목을 전달하는 채널
	Committed() <-chan *pb.LogEntry

	// 현재 리더의 노드 ID
	LeaderID() uint32

	// 현재 노드가 리더인지 여부
	IsLeader() bool

	// 노드가 합의에 이르면 마지막으로 적용한 인덱스 설정을 위해 호출
	SetLastApplied(index uint64)

	// 각 고루틴을 시작(리더 선출, 리더의 하트비트 등)
	Start() error

	// 동작을 중지하고 리소스 해제
	Stop()
}
