package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ClusterStatus represents the operational status reported by /status.
type ClusterStatus struct {
	NodeID        string    `json:"node_id"`
	Role          string    `json:"role"`
	LeaderID      string    `json:"leader_id"`
	LeaderAddr    string    `json:"leader_addr"`
	Term          uint64    `json:"term"`
	CommitIndex   uint64    `json:"commit_index"`
	AppliedIndex  uint64    `json:"applied_index"`
	ActiveWorkers int       `json:"active_workers"`
	QueueDepth    int       `json:"queue_depth"`
	StartTime     time.Time `json:"start_time"`
	UptimeSeconds float64   `json:"uptime_seconds"`
}

// StatusProvider allows the coordinator server to supply live health and status info.
type StatusProvider interface {
	GetClusterStatus() ClusterStatus
	IsReady() bool
}

// HTTPServer exposes /healthz, /readyz, /status, and /metrics.
type HTTPServer struct {
	server   *http.Server
	listener net.Listener
	metrics  *Metrics
	provider StatusProvider
	addr     string
}

// NewHTTPServer initializes the HTTP telemetry endpoint listener.
func NewHTTPServer(addr string, m *Metrics, provider StatusProvider) *HTTPServer {
	if m == nil {
		m = DefaultMetrics
	}

	mux := http.NewServeMux()
	s := &HTTPServer{
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
		metrics:  m,
		provider: provider,
		addr:     addr,
	}

	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	return s
}

// Start launches the HTTP listener asynchronously.
func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("telemetry http listen error on %s: %w", s.server.Addr, err)
	}
	s.listener = ln
	s.addr = ln.Addr().String()

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Log or handle unexpected listener termination
		}
	}()
	return nil
}

// Addr returns the bound address (useful when port 0 is used in tests).
func (s *HTTPServer) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *HTTPServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	isReady := true
	if s.provider != nil {
		isReady = s.provider.IsReady()
	}

	if isReady {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ready",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "not_ready",
			"reason": "node is not ready or has lost quorum",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var status ClusterStatus
	if s.provider != nil {
		status = s.provider.GetClusterStatus()
	} else {
		status = ClusterStatus{
			NodeID: "unknown",
			Role:   "standalone",
		}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(status)
}
