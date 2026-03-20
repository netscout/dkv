package paxos

import (
	"sync"

	pb "dkv/transport/proto/dkvpb"
)

type ReplicatedLog struct {
	mu        sync.RWMutex
	entries   map[uint64]*pb.LogEntry // 슬롯 -> 로그 엔트리
	committed map[uint64]bool         // 슬롯 -> 커밋 여부
	maxSlot   uint64                  // 최대 슬롯 번호
}

func NewReplicatedLog() *ReplicatedLog {
	return &ReplicatedLog{
		entries:   make(map[uint64]*pb.LogEntry),
		committed: make(map[uint64]bool),
	}
}

func (l *ReplicatedLog) Set(slot uint64, entry *pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries[slot] = entry
	if slot > l.maxSlot {
		l.maxSlot = slot
	}
}

func (l *ReplicatedLog) Get(slot uint64) *pb.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.entries[slot]
}

func (l *ReplicatedLog) MarkCommitted(slot uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.committed[slot] = true
}

func (l *ReplicatedLog) IsCommitted(slot uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.committed[slot]
}

/*
중간에 끊어지지 않고 커밋된 마지막 슬롯 번호를 반환한다.
1,2,3,5 의 경우 3을 반환한다. -> 슬롯 4번은 비어있다.
*/
func (l *ReplicatedLog) CommittedUpTo() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var idx uint64
	for {
		if !l.committed[idx+1] {
			return idx
		}
		idx++
	}
}

// 다음 슬롯 번호를 반환한다.
func (l *ReplicatedLog) NextSlot() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxSlot++
	return l.maxSlot
}

// after 슬롯 이후에 커밋된 모든 로그 엔트리를 반환한다.
func (l *ReplicatedLog) EntriesAfter(after uint64) []*pb.LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []*pb.LogEntry
	for slot := after + 1; slot <= l.maxSlot; slot++ {
		if l.committed[slot] {
			if entry, ok := l.entries[slot]; ok {
				result = append(result, entry)
			}
		}
	}
	return result
}
