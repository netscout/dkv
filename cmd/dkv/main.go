package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"dkv/config"
	"dkv/node"
	"dkv/transport"
)

func main() {
	// -id=<1|2|3> 형식으로 노드 ID를 지정한다.
	nodeID := flag.Uint("id", 0, "Node ID (1, 2, or 3)")
	verbose := flag.Bool("verbose", false, "Enable debug-level logging")
	flag.Parse()

	if *nodeID < 1 || *nodeID > 3 {
		fmt.Fprintln(os.Stderr, "usage: dkv -id=<1|2|3> [-verbose]")
		os.Exit(1)
	}

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}

	// 기본 클러스터 설정을 가져온다.(노드 3개 클러스터)
	cfg := config.DefaultCluster(uint32(*nodeID))
	err := os.MkdirAll(cfg.DataDir, 0755)
	if err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// gRPC 통신을 위한 피어 주소 맵을 생성한다.
	peerAddrs := make(map[uint32]string)
	for _, p := range cfg.Peers {
		peerAddrs[p.NodeID] = p.RPCAddr
	}
	// outbound(다른 노드에 전송하는 통신)을 위한 gRPC 통신 객체를 생성한다.
	grpcTransport := transport.NewGRPCTransport(peerAddrs)

	// gRPC 통신을 사용하는 Paxos 노드를 생성한다.(node.go 참조)
	n, err := node.NewNode(cfg, grpcTransport, logLevel)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	// Paxos 노드가 inbound(다른 노드에서 인입되는 통신)을 위해 gRPC 서버를 시작한다.(transport/grpc_server.go 참조)
	// 인입되는 메세지는 paxos.MultiPaxos가 처리한다.
	grpcServer := transport.NewGRPCServer(n.GetMultiPaxosMessageHandler())
	go func() {
		log.Printf("gRPC listening on %s", cfg.RPCAddr)
		if err := grpcServer.Serve(cfg.RPCAddr); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// Paxos 노드를 시작한다. -> 리더 선출 루프 시작
	if err := n.Start(); err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	fmt.Printf("Node %d started — HTTP: %s, gRPC: %s\n", cfg.NodeID, cfg.HTTPAddr, cfg.RPCAddr)
	// SIGINT 또는 SIGTERM 신호가 수신될 때까지 대기한다.
	n.WaitForShutdown()
	// gRPC inbound 통신을 종료한다.
	grpcServer.Stop()
	// gRPC outbound 통신을 모두 종료한다.
	grpcTransport.Close()
}
