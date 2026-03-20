package transport

import (
	"context"
	pb "dkv/transport/proto/dkvpb"
	"fmt"
	"sync"
)

type MessageHandler interface {
	HandlePrepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error)
	HandleAccept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error)
	HandleCommit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error)
	HandleCatchup(ctx context.Context, req *pb.CatchupRequest) (*pb.CatchupResponse, error)
	HandleHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error)
}

// 메모리 기반으로 노드간의 통신을 구현하는 구조체
type InMemoryTransport struct {
	mu sync.RWMutex
	// 노드 ID에 해당하는 MessageHandler를 저장하는 맵
	handlers map[uint32]MessageHandler
	// 네트워크 장애를 시뮬레이션하기 위한 노드 상태
	disconnected map[uint32]bool
	// gRPC 백오프 시뮬레이션: 특정 노드로의 하트비트만 차단한다.
	// Prepare/Accept/Commit/Catchup 메시지는 영향받지 않는다.
	// 주의: 차단은 수신 노드(destination) 기준이며, 발신 노드(source) 기준이 아니다.
	// 현재 구현에서는 리더만 하트비트를 전송하므로(heartbeatLoop에서 isLeader 체크),
	// 수신 노드 기준 차단과 발신 노드 기준 차단의 결과가 동일하다.
	// 만약 향후 peer-to-peer 헬스 체크 등으로 하트비트 프로토콜이 변경되면
	// 발신 노드별 차단으로 수정이 필요하다.
	heartbeatBlockedTo map[uint32]bool
}

func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{
		handlers:           make(map[uint32]MessageHandler),
		disconnected:       make(map[uint32]bool),
		heartbeatBlockedTo: make(map[uint32]bool),
	}
}

// 노드 ID에 해당하는 MessageHandler를 등록한다.
func (t *InMemoryTransport) Register(nodeID uint32, handler MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[nodeID] = handler
}

// 네트워크 장애로 인해 접속 불가능한 상황을 시뮬레이션
func (t *InMemoryTransport) Disconnect(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disconnected[nodeID] = true
}

func (t *InMemoryTransport) Reconnect(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.disconnected, nodeID)
}

// gRPC 백오프 시뮬레이션: 특정 노드로의 하트비트를 차단한다.
// Prepare/Accept/Commit/Catchup 등 다른 메시지는 영향받지 않는다.
// 이는 gRPC 클라이언트가 연결 실패 후 백오프 상태에 들어가서
// 하트비트가 전달되지 않는 상황을 재현한다.
func (t *InMemoryTransport) BlockHeartbeatsTo(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.heartbeatBlockedTo[nodeID] = true
}

// 특정 노드로의 하트비트 차단을 해제한다.
// gRPC 백오프가 만료되어 연결이 복구된 상황을 재현한다.
// 참고: 차단되지 않은 노드에 대해 호출해도 안전하다.
// Go의 map delete는 존재하지 않는 키에 대해 no-op이다.
func (t *InMemoryTransport) UnblockHeartbeatsTo(nodeID uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.heartbeatBlockedTo, nodeID)
}

// 노드 ID에 해당하는 MessageHandler를 반환한다.
// 해당 노드가 InMemoryTransport에 등록되어 있지 않거나, 네트워크 장애로 인해 접속 불가능한 경우 에러를 반환한다.
func (t *InMemoryTransport) getHandler(to uint32) (MessageHandler, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.disconnected[to] {
		return nil, fmt.Errorf("node %d is disconnected", to)
	}
	handler, ok := t.handlers[to]
	if !ok {
		return nil, fmt.Errorf("node %d not registered", to)
	}
	return handler, nil
}

func (t *InMemoryTransport) SendPrepare(ctx context.Context, to uint32, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	handler, err := t.getHandler(to)
	if err != nil {
		return nil, fmt.Errorf("send prepare to %d: %w", to, err)
	}
	return handler.HandlePrepare(ctx, req)
}

func (t *InMemoryTransport) SendAccept(ctx context.Context, to uint32, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	handler, err := t.getHandler(to)
	if err != nil {
		return nil, fmt.Errorf("send accept to %d: %w", to, err)
	}
	return handler.HandleAccept(ctx, req)
}

func (t *InMemoryTransport) SendCommit(ctx context.Context, to uint32, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	handler, err := t.getHandler(to)
	if err != nil {
		return nil, fmt.Errorf("send commit to %d: %w", to, err)
	}
	return handler.HandleCommit(ctx, req)
}

func (t *InMemoryTransport) SendCatchup(ctx context.Context, to uint32, req *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	handler, err := t.getHandler(to)
	if err != nil {
		return nil, fmt.Errorf("send catchup to %d: %w", to, err)
	}
	return handler.HandleCatchup(ctx, req)
}

func (t *InMemoryTransport) SendHeartbeat(ctx context.Context, to uint32, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// gRPC 백오프 시뮬레이션: 해당 노드로의 하트비트가 차단된 경우 에러를 반환한다.
	t.mu.RLock()
	blocked := t.heartbeatBlockedTo[to]
	t.mu.RUnlock()
	if blocked {
		return nil, fmt.Errorf("send heartbeat to %d: heartbeat blocked (simulating gRPC backoff)", to)
	}

	handler, err := t.getHandler(to)
	if err != nil {
		return nil, fmt.Errorf("send heartbeat to %d: %w", to, err)
	}
	return handler.HandleHeartbeat(ctx, req)
}
