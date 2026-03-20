package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "dkv/transport/proto/dkvpb"
)

type GRPCTransport struct {
	// conns, clients의 동시성 제어를 위해 사용
	mu sync.RWMutex
	// 각 노드별 gRPC 커넥션 맵 -> 명시적으로 커넥션을 종료하기 위해 사용한다.
	// 첫 번째 요청시 커넥션을 생성한다(lazy initialization)
	conns map[uint32]*grpc.ClientConn
	// 각 노드별 gRPC 클라이언트 맵
	// 첫 번째 요청시 클라이언트를 생성한다(lazy initialization)
	clients map[uint32]pb.DKVConsensusClient
	// 각 노드별 "host:port" 주소 맵
	addrs map[uint32]string
}

// GRPCTransport가 Transport 인터페이스를 구현하고 있는지 확인한다.
var _ Transport = (*GRPCTransport)(nil)

func NewGRPCTransport(peerAddrs map[uint32]string) *GRPCTransport {
	return &GRPCTransport{
		conns:   make(map[uint32]*grpc.ClientConn),
		clients: make(map[uint32]pb.DKVConsensusClient),
		addrs:   peerAddrs,
	}
}

func (t *GRPCTransport) getClient(to uint32) (pb.DKVConsensusClient, error) {
	// 노드의 gRPC 클라이언트가 이미 존재하는지 확인하기 위해 읽기 락을 사용한다.(fast lock)
	t.mu.RLock()
	if c, ok := t.clients[to]; ok {
		t.mu.RUnlock()
		return c, nil
	}
	t.mu.RUnlock()

	// 노드의 gRPC 클라이언트가 존재하지 않는다면, 쓰기 락을 사용하여 생성한다.(slow lock)
	t.mu.Lock()
	defer t.mu.Unlock()

	// 노드의 gRPC 클라이언트가 이미 존재하는지 다시 확인한다.
	if c, ok := t.clients[to]; ok {
		return c, nil
	}

	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", to)
	}

	// tls 없이 gRPC 커넥션을 생성한다.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial peer %d at %s: %w", to, addr, err)
	}

	client := pb.NewDKVConsensusClient(conn)
	t.conns[to] = conn
	t.clients[to] = client
	return client, nil
}

func (t *GRPCTransport) SendPrepare(ctx context.Context, to uint32, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	c, err := t.getClient(to)
	if err != nil {
		return nil, err
	}
	return c.Prepare(ctx, req)
}

func (t *GRPCTransport) SendAccept(ctx context.Context, to uint32, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	c, err := t.getClient(to)
	if err != nil {
		return nil, err
	}
	return c.Accept(ctx, req)
}

func (t *GRPCTransport) SendCommit(ctx context.Context, to uint32, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	c, err := t.getClient(to)
	if err != nil {
		return nil, err
	}
	return c.Commit(ctx, req)
}

func (t *GRPCTransport) SendCatchup(ctx context.Context, to uint32, req *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	c, err := t.getClient(to)
	if err != nil {
		return nil, err
	}
	return c.Catchup(ctx, req)
}

func (t *GRPCTransport) SendHeartbeat(ctx context.Context, to uint32, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	c, err := t.getClient(to)
	if err != nil {
		return nil, err
	}
	return c.Heartbeat(ctx, req)
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
}
