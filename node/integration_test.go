package node

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"dkv/config"
	"dkv/transport"
)

func TestThreeNodeCluster(t *testing.T) {
	// Clean up data dirs.
	os.RemoveAll("/tmp/dkv-test")
	defer os.RemoveAll("/tmp/dkv-test")

	tr := transport.NewInMemoryTransport()

	// Create and start 3 nodes.
	nodes := make([]*Node, 3)
	for i := uint32(1); i <= 3; i++ {
		cfg := &config.Config{
			NodeID: i,
			Peers: []config.PeerConfig{
				{NodeID: 1, RPCAddr: "127.0.0.1:19001"},
				{NodeID: 2, RPCAddr: "127.0.0.1:19002"},
				{NodeID: 3, RPCAddr: "127.0.0.1:19003"},
			},
			DataDir:          fmt.Sprintf("/tmp/dkv-test/node-%d", i),
			HTTPAddr:         fmt.Sprintf("127.0.0.1:%d", 18000+i),
			RPCAddr:          fmt.Sprintf("127.0.0.1:%d", 19000+i),
			SnapshotInterval: 50,
		}
		n, err := NewNode(cfg, tr, slog.LevelInfo)
		if err != nil {
			t.Fatal(err)
		}
		tr.Register(i, n.GetMultiPaxosMessageHandler())
		nodes[i-1] = n
	}

	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.consensus.Stop()
			n.wal.Close()
		}
	}()

	// Wait for leader election.
	var leaderHTTP string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.consensus.IsLeader() {
				leaderHTTP = n.cfg.HTTPAddr
				t.Logf("leader: node %d at %s", n.cfg.NodeID, leaderHTTP)
				goto leaderFound
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no leader elected")
leaderFound:

	// PUT some keys via the leader.
	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		url := fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key)
		req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(val))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("PUT %s: %v", key, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s: status %d", key, resp.StatusCode)
		}
	}

	// Wait for replication.
	time.Sleep(1 * time.Second)

	// GET from all nodes (stale reads, but should have replicated by now).
	for _, n := range nodes {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			expected := fmt.Sprintf("val%d", i)
			url := fmt.Sprintf("http://%s/kv/%s", n.cfg.HTTPAddr, key)
			resp, err := client.Get(url)
			if err != nil {
				t.Fatalf("GET %s from node %d: %v", key, n.cfg.NodeID, err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(body) != expected {
				t.Errorf("node %d: GET %s = %q, want %q", n.cfg.NodeID, key, body, expected)
			}
		}
	}
	t.Log("all 20 keys replicated to all 3 nodes")
}
