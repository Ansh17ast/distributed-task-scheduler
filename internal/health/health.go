package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// Server provides liveness and readiness probes for coordinators and workers.
type Server struct {
	isReady     atomic.Bool
	isLeader    atomic.Bool
	nodeID      string
	healthCheck func() map[string]interface{}
}

// NewServer creates a new HealthServer instance.
func NewServer(nodeID string, healthCheck func() map[string]interface{}) *Server {
	s := &Server{
		nodeID:      nodeID,
		healthCheck: healthCheck,
	}
	s.isReady.Store(true)
	return s
}

// SetReady updates the readiness status.
func (s *Server) SetReady(ready bool) {
	s.isReady.Store(ready)
}

// SetLeader updates the leadership status.
func (s *Server) SetLeader(leader bool) {
	s.isLeader.Store(leader)
}

// RegisterRoutes registers /healthz, /readyz, and /status endpoints on the provided mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"node_id": s.nodeID,
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !s.isReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "not_ready",
			"node_id": s.nodeID,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ready",
		"node_id": s.nodeID,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	status := map[string]interface{}{
		"node_id":   s.nodeID,
		"ready":     s.isReady.Load(),
		"is_leader": s.isLeader.Load(),
	}
	if s.healthCheck != nil {
		for k, v := range s.healthCheck() {
			status[k] = v
		}
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(status)
}
