package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"

	pb "dkv/transport/proto/dkvpb"
)

// gRPC 요청을 처리할 메세지 핸들러를 포함하는 서버
type GRPCServer struct {
	// Forward compatibility
	// ./proto/dkv.proto에 새로운 RPC 메서드가 추가될 경우, 이를 구현하지 않아도 컴파일 오류가 발생하지 않도록 한다.
	pb.UnimplementedDKVConsensusServer
	// 인입되는 RPC 요청을 처리한다.(Paxos의 경우 MultiPaxosNode)
	handler MessageHandler
	server  *grpc.Server
}

func NewGRPCServer(handler MessageHandler) *GRPCServer {
	s := &GRPCServer{handler: handler}
	s.server = grpc.NewServer()
	// gRPC 서비스 구현을 등록한다.(RPC를 모두 구현한 GRPCServer 인스턴스를 등록)
	pb.RegisterDKVConsensusServer(s.server, s)
	return s
}

// gRPC 서버를 시작하는 함수(main.go에서 고루틴으로 실행된다)
func (s *GRPCServer) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// Stop()이 호출될 때까지 대기한다.
	return s.server.Serve(lis)
}

// 모든 활성 RPC 요청이 완료될 때까지 대기한 후 종료한다.
func (s *GRPCServer) Stop() {
	s.server.GracefulStop()
}

func (s *GRPCServer) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	return s.handler.HandlePrepare(ctx, req)
}

func (s *GRPCServer) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	return s.handler.HandleAccept(ctx, req)
}

func (s *GRPCServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	return s.handler.HandleCommit(ctx, req)
}

func (s *GRPCServer) Catchup(ctx context.Context, req *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	return s.handler.HandleCatchup(ctx, req)
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return s.handler.HandleHeartbeat(ctx, req)
}
