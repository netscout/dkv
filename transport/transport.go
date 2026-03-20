package transport

import (
	"context"
	pb "dkv/transport/proto/dkvpb"
)

// 노드 간의 통신을 위한 인터페이스
// 테스트에서는 InMemoryTransport를, 실제 구현에서는 GRPCTransport를 사용
type Transport interface {
	SendPrepare(ctx context.Context, to uint32, req *pb.PrepareRequest) (*pb.PrepareResponse, error)
	SendAccept(ctx context.Context, to uint32, req *pb.AcceptRequest) (*pb.AcceptResponse, error)
	SendCommit(ctx context.Context, to uint32, req *pb.CommitRequest) (*pb.CommitResponse, error)
	SendCatchup(ctx context.Context, to uint32, req *pb.CatchupRequest) (*pb.CatchupResponse, error)
	SendHeartbeat(ctx context.Context, to uint32, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
}
