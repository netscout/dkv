package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"dkv/config"
	"dkv/consensus"
	"dkv/statemachine"
	pb "dkv/transport/proto/dkvpb"
)

type HTTPServer struct {
	cfg       *config.Config
	consensus consensus.Consensus
	kv        *statemachine.KVStore
	log       *slog.Logger
	mux       *http.ServeMux
	server    *http.Server
}

// KV 스토어 명령을 처리하는 HTTP 서버를 생성하는 함수
func NewHTTPServer(cfg *config.Config, c consensus.Consensus, kv *statemachine.KVStore, logger *slog.Logger) *HTTPServer {
	s := &HTTPServer{
		cfg:       cfg,
		consensus: c,
		kv:        kv,
		log:       logger,
		mux:       http.NewServeMux(),
	}

	s.mux.HandleFunc("/kv/", s.handleKV)
	s.mux.HandleFunc("/debug/state", s.handleDebugState)

	s.server = &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: s.mux,
	}
	return s
}

// HTTP 서버를 시작하는 함수(node.go에서 고루틴으로 실행된다)
func (s *HTTPServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

// KV 스토어 명령을 처리하는 HTTP 서버의 핸들러 함수
func (s *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// KV스토어에서 특정 키의 값을 조회하는 GET 명령을 처리하는 함수
// 결과적 일관성을 통한 읽기(Stale read)를 허용한다.
func (s *HTTPServer) handleGet(w http.ResponseWriter, key string) {
	v, ok := s.kv.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(v))
}

// KV스토어에 특정 키의 값을 설정하는 PUT 명령을 처리하는 함수
func (s *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	if !s.consensus.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	idx, err := s.consensus.Propose(ctx, &pb.Command{
		Op:    "PUT",
		Key:   key,
		Value: string(body),
	})
	if err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK (index=%d)", idx)
}

// KV스토어에서 특정 키의 값을 삭제하는 DELETE 명령을 처리하는 함수
func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if !s.consensus.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	_, err := s.consensus.Propose(ctx, &pb.Command{Op: "DEL", Key: key})
	if err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// 리더로 리다이렉트하는 함수
// 현재는 요청을 직접 리다이렉트 하지 않고, 리더의 HTTP 주소를 반환하도록 한다.
func (s *HTTPServer) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := s.consensus.LeaderID()
	if leaderID == 0 {
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}
	// 리더의 HTTP 주소를 찾고, 리다이렉트 한다.
	for _, p := range s.cfg.Peers {
		if p.NodeID == leaderID {
			leaderHTTP := fmt.Sprintf("http://127.0.0.1:%d", 8000+leaderID)
			http.Redirect(w, r, leaderHTTP+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}
	}
	http.Error(w, "leader unknown", http.StatusServiceUnavailable)
}

// 디버깅 정보를 조회하는 함수
func (s *HTTPServer) handleDebugState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "node_id: %d\n", s.cfg.NodeID)
	fmt.Fprintf(w, "is_leader: %v\n", s.consensus.IsLeader())
	fmt.Fprintf(w, "leader_id: %d\n", s.consensus.LeaderID())
	fmt.Fprintf(w, "last_applied: %d\n", s.kv.LastApplied())
}
