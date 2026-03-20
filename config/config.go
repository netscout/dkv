package config

import "fmt"

type PeerConfig struct {
	NodeID  uint32
	RPCAddr string
}

type Config struct {
	NodeID   uint32
	Peers    []PeerConfig // 노드 자기 자신을 포함한 모든 노드 정보
	DataDir  string       // WAL 파일 및 스냅샷 파일 등이 저장될 경로
	HTTPAddr string       // 클라이언트가 접속할 HTTP 서버 주소
	RPCAddr  string       // 노드 간 통신을 위한 RPC 서버 주소

	SnapshotInterval uint64 // 스냅샷 생성 간격 (이전 스냅샷 이후에 커밋된 엔트리 개수)
}

// 기본 클러스터 설정 반환
func DefaultCluster(nodeId uint32) *Config {
	peers := []PeerConfig{
		{NodeID: 1, RPCAddr: "127.0.0.1:9001"},
		{NodeID: 2, RPCAddr: "127.0.0.1:9002"},
		{NodeID: 3, RPCAddr: "127.0.0.1:9003"},
	}

	return &Config{
		NodeID:           nodeId,
		Peers:            peers,
		DataDir:          fmt.Sprintf("/tmp/dkv/node-%d", nodeId),
		HTTPAddr:         fmt.Sprintf("127.0.0.1:%d", 8000+nodeId),
		RPCAddr:          fmt.Sprintf("127.0.0.1:%d", 9000+nodeId),
		SnapshotInterval: 100,
	}
}

// 클러스터의 과반수 노드 개수
func (c *Config) Majority() int {
	return len(c.Peers)/2 + 1
}

// 모든 노드의 NodeID 목록을 반환
func (c *Config) PeerIDs() []uint32 {
	ids := make([]uint32, 0, len(c.Peers))
	for _, peer := range c.Peers {
		ids = append(ids, peer.NodeID)
	}
	return ids
}
