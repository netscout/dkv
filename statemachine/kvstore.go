package statemachine

import (
	"sync"

	pb "dkv/transport/proto/dkvpb"
)

// key-value 스토어 구조체
type KVStore struct {
	mu          sync.RWMutex
	data        map[string]string
	lastApplied uint64
}

// 새로운 key-value 스토어 인스턴스를 생성하는 함수
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// 커밋 완료된 로그 항목을 실행하고, 결과를 반환하는 함수(GET에 대해서는 값을, 나머지 명령은 "" 반환)
// 이미 적용된 명령은 무시하고 ""를 반환하므로, 멱등성(Idempotent)을 보장
func (kv *KVStore) Apply(entry *pb.LogEntry) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if entry.Index <= kv.lastApplied {
		return "" // 이미 적용된 명령이므로 무시
	}

	var result string
	cmd := entry.Command

	switch cmd.Op {
	case "GET":
		result = kv.data[cmd.Key]
	case "PUT":
		kv.data[cmd.Key] = cmd.Value
	case "DEL":
		delete(kv.data, cmd.Key)
	case "NOOP":
		// no-op, 리더 확인을 위해 사용됨
	}

	kv.lastApplied = entry.Index
	return result
}

// 스토어에서 특정 키의 값을 조회하는 함수
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.data[key]
	return value, ok
}

// KV스토어의 현재 상태를 스냅샷 데이터로 반환하는 함수
func (kv *KVStore) Snapshot() *pb.SnapshotData {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	clone := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		clone[k] = v
	}

	return &pb.SnapshotData{
		LastIndex: kv.lastApplied,
		KvData:    clone,
	}
}

// 스냅샷 데이터를 사용하여 스토어를 복원하는 함수
func (kv *KVStore) Restore(snap *pb.SnapshotData) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data = make(map[string]string, len(snap.KvData))
	for k, v := range snap.KvData {
		kv.data[k] = v
	}
	kv.lastApplied = snap.LastIndex
}

// 현재 상태의 마지막 적용된 인덱스 번호를 반환하는 함수
func (kv *KVStore) LastApplied() uint64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.lastApplied
}
