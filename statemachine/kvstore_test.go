package statemachine

import (
	"testing"

	pb "dkv/transport/proto/dkvpb"
)

// PUT 명령을 적용하면 키에 값이 저장되어야 함
func TestApplyPutGetDel(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(&pb.LogEntry{
		Index:   1,
		Command: &pb.Command{Op: "PUT", Key: "name", Value: "alice"},
	})

	v, ok := kv.Get("name")
	if !ok || v != "alice" {
		t.Errorf("expected value 'alice', got '%s'", v)
	}
}

// 동일한 인덱스를 가진 다른 데이터를 Apply하면 멱등성이 보장되어야 함
func TestApplyIdempotent(t *testing.T) {
	kv := NewKVStore()

	entry := &pb.LogEntry{
		Index:   1,
		Command: &pb.Command{Op: "PUT", Key: "x", Value: "first"},
	}
	kv.Apply(entry)

	// 동일한 인덱스를 가진 다른 데이터를 Apply
	entry2 := &pb.LogEntry{
		Index:   1,
		Command: &pb.Command{Op: "PUT", Key: "x", Value: "second"},
	}
	kv.Apply(entry2)

	v, _ := kv.Get("x")
	if v != "first" {
		t.Errorf("idempotency broken: expected value 'first', got '%s'", v)
	}
}

// 삭제 명령을 적용하면 키가 삭제되어야 함
func TestApplyDelete(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(&pb.LogEntry{
		Index:   1,
		Command: &pb.Command{Op: "PUT", Key: "x", Value: "first"},
	})

	kv.Apply(&pb.LogEntry{
		Index:   2,
		Command: &pb.Command{Op: "DEL", Key: "x"},
	})

	_, ok := kv.Get("x")
	if ok {
		t.Errorf("key should be deleted")
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(&pb.LogEntry{
		Index:   1,
		Command: &pb.Command{Op: "PUT", Key: "x", Value: "first"},
	})
	kv.Apply(&pb.LogEntry{
		Index:   2,
		Command: &pb.Command{Op: "PUT", Key: "y", Value: "second"},
	})

	snap := kv.Snapshot()

	kv2 := NewKVStore()
	kv2.Restore(snap)

	v, _ := kv2.Get("x")
	if v != "first" {
		t.Errorf("snapshot and restore broken: expected value 'first', got '%s'", v)
	}
	if kv2.LastApplied() != 2 {
		t.Errorf("snapshot and restore broken: expected last applied 2, got %d", kv2.LastApplied())
	}
}
