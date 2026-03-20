package node

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	"dkv/config"
	"dkv/consensus"
	"dkv/consensus/paxos"
	"dkv/server"
	"dkv/statemachine"
	"dkv/storage/snapshot"
	"dkv/storage/wal"
	"dkv/transport"
	pb "dkv/transport/proto/dkvpb"
)

type Node struct {
	cfg       *config.Config
	consensus *paxos.MultiPaxos
	kv        *statemachine.KVStore
	wal       *wal.WAL
	snapDir   string
	http      *server.HTTPServer
	log       *slog.Logger
}

func NewNode(cfg *config.Config, t transport.Transport, logLevel slog.Level) (*Node, error) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})).With("node", cfg.NodeID)

	walDir := filepath.Join(cfg.DataDir, "wal")
	snapDir := filepath.Join(cfg.DataDir, "snap")

	w, err := wal.Open(walDir)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	kv := statemachine.NewKVStore()
	mp := paxos.NewMultiPaxos(cfg.NodeID, cfg.PeerIDs(), w, t, logger)

	n := &Node{
		cfg:       cfg,
		consensus: mp,
		kv:        kv,
		wal:       w,
		snapDir:   snapDir,
		log:       logger,
	}

	// HTTP 서버는 KV 스토어에 대한 PUT/DELETE 명령을 합의과정을 통해 처리해야 하므로, consensus.Consensus 구현체가 필요하다.(현재는 MultiPaxos)
	n.http = server.NewHTTPServer(cfg, n.consensusInterface(), kv, logger)

	return n, nil
}

// *MultiPaxos를 consensus.Consensus 인터페이스로 변환하여 반환한다. -> *MultiPaxos가 consensus.Consensus 인터페이스를 구현하고 있음을 명시적으로 표현한다.
func (n *Node) consensusInterface() consensus.Consensus {
	return n.consensus
}

func (n *Node) Start() error {
	/*
		1. 가장 최신의 snapshot을 로드한다.

		snapshot에는 KV스토어의 당시 상태가 파일로 저장되어 있다.
	*/
	snap, err := snapshot.Latest(n.snapDir)
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	if snap != nil {
		n.kv.Restore(snap) // KVStore에 저장된 데이터를 snapshot의 데이터로 덮어쓴다.
		n.log.Info("snapshot: 스냅샷 복원", "lastIndex", snap.LastIndex)
	}

	/*
		2. WAL 파일을 재생(replay)한다.

		모든 로그 엔트리는 KV스토어에 적용되기 전에 WAL 파일에 저장된다.
		만약 WAL파일에는 있지만 KV스토어에 적용되기 전(또는 snapshot에 저장되기 전)에 노드가 다운되었다면,
		해당 로그 엔트리들을 WAL에서 재생(replay)하여 KV스토어에 적용한다.
	*/
	records, err := n.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("read wal: %w", err)
	}
	// WAL 재생 통계 (trivial overhead: 두 개의 int 변수)
	acceptorStateCount := 0
	committedEntryCount := 0
	for _, raw := range records {
		rec := &pb.WALRecord{}
		if err := proto.Unmarshal(raw, rec); err != nil {
			n.log.Warn("wal: 손상된 WAL 레코드 건너뜀", "err", err)
			continue
		}
		switch rec.Type {
		case pb.WALRecord_ACCEPTOR_STATE: // Paxos acceptor의 상태를 복원한다.
			acceptorStateCount++
			n.log.Debug("wal: acceptor 상태 레코드 발견",
				"slot", rec.AcceptorState.Slot,
				"promisedBallot", paxos.BallotString(rec.AcceptorState.PromisedBallot),
				"acceptedBallot", paxos.BallotString(rec.AcceptorState.AcceptedBallot))
			n.consensus.GetAcceptor().RestoreSlot(rec.AcceptorState)
		case pb.WALRecord_COMMITTED_ENTRY: // KV스토어에 명령을 적용한다.
			committedEntryCount++
			n.log.Debug("wal: 커밋 엔트리 레코드 발견",
				"index", rec.CommittedEntry.Index,
				"op", rec.CommittedEntry.Command.Op,
				"key", rec.CommittedEntry.Command.Key)
			n.kv.Apply(rec.CommittedEntry)
		}
	}
	n.log.Info("wal: 재생 완료",
		"lastApplied", n.kv.LastApplied(),
		"totalRecords", len(records),
		"acceptorStates", acceptorStateCount,
		"committedEntries", committedEntryCount)
	n.consensus.SetLastApplied(n.kv.LastApplied())

	/*
		3. Paxos 프로토콜을 시작한다.

		리더 선출 루프와 하트비트 루프를 시작한다.
	*/
	if err := n.consensus.Start(); err != nil {
		return err
	}

	/*
		4. 백그라운드 루프를 시작한다.
	*/
	go n.applyLoop()    // 리더가 엔트리를 커밋 후 commitCh로 전달하면, 이를 applyLoop가 수신하여 WAL -> KV스토어로 적용한다.
	go n.snapshotLoop() // 주기적으로 snapshot을 생성하고 WAL 파일을 초기화 한다.

	/*
		5. KV 스토어 명령을 처리하는 HTTP 서버를 시작한다.
	*/
	n.log.Info("http: HTTP 서버 시작", "addr", n.cfg.HTTPAddr)
	go func() {
		if err := n.http.ListenAndServe(); err != nil {
			n.log.Error("http: HTTP 서버 오류", "err", err)
		}
	}()

	return nil
}

// MultiPaxos의 commitCh을 통해 커밋된 엔트리를 수신하여, WAL에 저장하고 KV스토어에 적용한다.
func (n *Node) applyLoop() {
	/*
		MultiPaxos의 commitCh을 통해 커밋된 엔트리를 수신한다.
	*/
	for entry := range n.consensus.Committed() {
		// 1. KV스토어에 적용하기 전에, 커밋된 엔트리를 WAL에 저장한다.
		rec := &pb.WALRecord{
			Type:           pb.WALRecord_COMMITTED_ENTRY,
			CommittedEntry: entry,
		}
		data, err := proto.Marshal(rec)
		if err != nil {
			n.log.Error("wal: 커밋 엔트리 직렬화 실패", "err", err)
			continue
		}
		if err := n.wal.Append(data); err != nil {
			n.log.Error("wal: 커밋 엔트리 추가 실패", "err", err)
			continue
		}

		// 2. 커밋된 엔트리를 KV스토어에 적용한다.
		n.log.Debug("apply: 엔트리 적용",
			"slot", entry.Index,
			"op", entry.Command.Op,
			"key", entry.Command.Key)
		n.kv.Apply(entry)

		// 3. MultiPaxos의 lastApplied 상태를 업데이트한다.
		n.consensus.SetLastApplied(entry.Index)
	}
}

// 주기적으로 snapshot 파일을 생성하고 WAL 파일을 초기화 한다.(snapshot.go 참조)
func (n *Node) snapshotLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	lastSnap := n.kv.LastApplied()

	for range ticker.C {
		current := n.kv.LastApplied()
		if current-lastSnap >= n.cfg.SnapshotInterval {
			snap := n.kv.Snapshot()
			if err := snapshot.Save(n.snapDir, snap); err != nil {
				n.log.Error("snapshot: 스냅샷 저장 실패", "err", err)
				continue
			}
			n.wal.Reset() // 스냅샷 기록이후 더 이상 필요하지 않은 WAL 파일을 초기화 한다.
			n.log.Info("snapshot: 스냅샷 생성", "index", snap.LastIndex)
			lastSnap = current
		}
	}
}

// SIGINT 또는 SIGTERM 신호가 수신될 때 까지 대기한다.
func (n *Node) WaitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	n.log.Info("node: 종료 중...")
	n.consensus.Stop()
	n.wal.Close()
}

// transport 계층에서 사용할 수 있도록 paxos.MultiPaxosNode를 리턴한다.
func (n *Node) GetMultiPaxosMessageHandler() *paxos.MultiPaxos {
	return n.consensus
}
