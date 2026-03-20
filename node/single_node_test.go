package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"dkv/statemachine"
	"dkv/storage/snapshot"
	"dkv/storage/wal"
	pb "dkv/transport/proto/dkvpb"

	"google.golang.org/protobuf/proto"
)

func TestSingleNodePersistence(t *testing.T) {
	dir, _ := os.MkdirTemp("", "single-node-*")
	defer os.RemoveAll(dir)

	walDir := filepath.Join(dir, "wal")
	snapDir := filepath.Join(dir, "snap")

	// 세션 1. 데이터 추가, 스냅샷 저장, 추가 데이터 저장 후 크래시.
	w, _ := wal.Open(walDir)
	kv := statemachine.NewKVStore()

	// 1~5 번 명령 적용
	for i := uint64(1); i <= 5; i++ {
		entry := &pb.LogEntry{
			Index:   i,
			Command: &pb.Command{Op: "PUT", Key: fmt.Sprintf("k0%d", i), Value: fmt.Sprintf("%d", i)},
		}
		rec := &pb.WALRecord{
			Type:           pb.WALRecord_COMMITTED_ENTRY,
			CommittedEntry: entry,
		}
		data, _ := proto.Marshal(rec)
		w.Append(data)
		kv.Apply(entry)
	}

	// 스냅샷 저장
	snap := kv.Snapshot()
	snapshot.Save(snapDir, snap)

	// WAL 파일 리셋
	w.Reset()

	// 6-8 번 명령 적용(6-8 번은 WAL 파일에만 저장되고, 스냅샷에는 저장되지 않음)
	for i := uint64(6); i <= 8; i++ {
		entry := &pb.LogEntry{
			Index:   i,
			Command: &pb.Command{Op: "PUT", Key: fmt.Sprintf("k0%d", i), Value: fmt.Sprintf("%d", i)},
		}
		rec := &pb.WALRecord{
			Type:           pb.WALRecord_COMMITTED_ENTRY,
			CommittedEntry: entry,
		}
		data, _ := proto.Marshal(rec)
		w.Append(data)
		kv.Apply(entry)
	}
	w.Close()

	// 세션 2. 스냅샷을 복원 + WAL 파일 재생(replay)

	kv2 := statemachine.NewKVStore()

	// 스냅샷 복원
	snapData, err := snapshot.Latest(snapDir)
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}
	if snapData == nil {
		t.Fatalf("snapshot not found")
	}
	kv2.Restore(snapData)

	if kv2.LastApplied() != 5 {
		t.Fatalf("last applied index mismatch: expected 5, got %d", kv2.LastApplied())
	}

	// WAL 파일을 재생하기
	w2, _ := wal.Open(walDir)
	records, _ := w2.ReadAll()
	for _, raw := range records {
		rec := &pb.WALRecord{}
		proto.Unmarshal(raw, rec)
		if rec.Type == pb.WALRecord_COMMITTED_ENTRY {
			kv2.Apply(rec.CommittedEntry)
		}
	}
	w2.Close()

	// 3. 8개의 명령이 존재하는지 확인
	for i := uint64(1); i <= 8; i++ {
		key := fmt.Sprintf("k0%d", i)
		if _, ok := kv2.Get(key); !ok {
			t.Fatalf("key %s not found", key)
		}
	}

	if kv2.LastApplied() != 8 {
		t.Fatalf("last applied index mismatch: expected 8, got %d", kv2.LastApplied())
	}
}
