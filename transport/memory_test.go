package transport

import (
	"context"
	"testing"

	pb "dkv/transport/proto/dkvpb"
)

// mockHandler는 MessageHandler 인터페이스의 최소 구현이다.
// 하트비트 차단 테스트에서 핸들러 등록용으로만 사용한다.
type mockHandler struct{}

func (m *mockHandler) HandlePrepare(_ context.Context, _ *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	return &pb.PrepareResponse{Ok: true}, nil
}

func (m *mockHandler) HandleAccept(_ context.Context, _ *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	return &pb.AcceptResponse{Ok: true}, nil
}

func (m *mockHandler) HandleCommit(_ context.Context, _ *pb.CommitRequest) (*pb.CommitResponse, error) {
	return &pb.CommitResponse{}, nil
}

func (m *mockHandler) HandleCatchup(_ context.Context, _ *pb.CatchupRequest) (*pb.CatchupResponse, error) {
	return &pb.CatchupResponse{}, nil
}

func (m *mockHandler) HandleHeartbeat(_ context.Context, _ *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Success: true}, nil
}

// TestBlockHeartbeatsTo_BlocksOnlyHeartbeats: BlockHeartbeatsTo가 하트비트만 차단하고
// 다른 메시지 타입은 영향받지 않음을 검증한다.
func TestBlockHeartbeatsTo_BlocksOnlyHeartbeats(t *testing.T) {
	tr := NewInMemoryTransport()
	tr.Register(1, &mockHandler{})
	ctx := context.Background()

	// 차단 전: 하트비트 전송 성공
	_, err := tr.SendHeartbeat(ctx, 1, &pb.HeartbeatRequest{LeaderId: 2, Ballot: &pb.Ballot{Number: 1, NodeId: 2}})
	if err != nil {
		t.Fatalf("차단 전 하트비트가 실패함: %v", err)
	}

	// 차단 설정
	tr.BlockHeartbeatsTo(1)

	// 차단 후: 하트비트 전송 실패
	_, err = tr.SendHeartbeat(ctx, 1, &pb.HeartbeatRequest{LeaderId: 2, Ballot: &pb.Ballot{Number: 1, NodeId: 2}})
	if err == nil {
		t.Fatal("차단된 노드에 하트비트가 성공함, 실패해야 함")
	}

	// 차단 후: Prepare는 여전히 성공
	_, err = tr.SendPrepare(ctx, 1, &pb.PrepareRequest{Ballot: &pb.Ballot{Number: 1, NodeId: 2}})
	if err != nil {
		t.Fatalf("차단 후 Prepare가 실패함: %v (하트비트만 차단되어야 함)", err)
	}

	// 차단 해제
	tr.UnblockHeartbeatsTo(1)

	// 차단 해제 후: 하트비트 전송 성공
	_, err = tr.SendHeartbeat(ctx, 1, &pb.HeartbeatRequest{LeaderId: 2, Ballot: &pb.Ballot{Number: 1, NodeId: 2}})
	if err != nil {
		t.Fatalf("차단 해제 후 하트비트가 실패함: %v", err)
	}
}

// TestUnblockHeartbeatsTo_NonBlockedNode: 차단되지 않은 노드에 대해 UnblockHeartbeatsTo를
// 호출해도 패닉이 발생하지 않음을 검증한다.
func TestUnblockHeartbeatsTo_NonBlockedNode(t *testing.T) {
	tr := NewInMemoryTransport()
	// 차단된 적 없는 노드에 대해 차단 해제 호출 -- Go map delete는 no-op이므로 안전
	tr.UnblockHeartbeatsTo(999)
	// 패닉 없이 정상 종료되면 테스트 통과
}
