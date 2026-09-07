package chaos

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/coordinator"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HarnessConfig specifies chaos experiment parameters.
type HarnessConfig struct {
	ExperimentID string
	Seed         int64
	NodeCount    int
	UseTCP       bool
	DataDir      string
	HeartbeatDur time.Duration
	ElectionMin  time.Duration
	ElectionMax  time.Duration
	WorkerLease  time.Duration
	ReconcileDur time.Duration
}

// DefaultHarnessConfig returns standard fast-timing config for chaos tests.
func DefaultHarnessConfig(expID string, seed int64) *HarnessConfig {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &HarnessConfig{
		ExperimentID: expID,
		Seed:         seed,
		NodeCount:    3,
		UseTCP:       false,
		HeartbeatDur: 20 * time.Millisecond,
		ElectionMin:  60 * time.Millisecond,
		ElectionMax:  120 * time.Millisecond,
		WorkerLease:  2 * time.Second,
		ReconcileDur: 1 * time.Second,
	}
}

// ChaosHarness is the unified coordinator cluster orchestration and fault-injection platform.
type ChaosHarness struct {
	Cfg        *HarnessConfig
	Journal    *Journal
	Invariants *InvariantChecker
	Injector   *FaultInjector

	nodes      []*consensus.RaftNode
	servers    []*coordinator.Server
	stores     []*state.Store
	transports []*raft.InmemTransport
	listeners  []net.Listener
	grpcAddrs  []string
	conns      []*grpc.ClientConn
	clients    []schedulerv1.CoordinatorServiceClient
	wClients   []schedulerv1.WorkerServiceClient
	peers      []consensus.RaftPeer

	tempDir string
	mu      sync.RWMutex
	closed  bool
}

// NewChaosHarness initializes and starts a chaos-controlled coordinator cluster.
func NewChaosHarness(cfg *HarnessConfig) (*ChaosHarness, error) {
	if cfg == nil {
		cfg = DefaultHarnessConfig("chaos-default", 42)
	}

	journal := NewJournal(cfg.ExperimentID, cfg.Seed)
	invariants := NewInvariantChecker()

	count := cfg.NodeCount
	if count <= 0 {
		count = 3
	}

	h := &ChaosHarness{
		Cfg:        cfg,
		Journal:    journal,
		Invariants: invariants,
		nodes:      make([]*consensus.RaftNode, count),
		servers:    make([]*coordinator.Server, count),
		stores:     make([]*state.Store, count),
		transports: make([]*raft.InmemTransport, count),
		listeners:  make([]net.Listener, count),
		grpcAddrs:  make([]string, count),
		conns:      make([]*grpc.ClientConn, count),
		clients:    make([]schedulerv1.CoordinatorServiceClient, count),
		wClients:   make([]schedulerv1.WorkerServiceClient, count),
		peers:      make([]consensus.RaftPeer, count),
	}

	journal.Record(EventExperimentStarted, "", "", "", 0, 0,
		"Cluster harness initializing: nodes=%d, useTCP=%v, seed=%d", count, cfg.UseTCP, cfg.Seed)

	if cfg.UseTCP && cfg.DataDir == "" {
		td, err := os.MkdirTemp("", fmt.Sprintf("chaos-tcp-%s-*", cfg.ExperimentID))
		if err != nil {
			return nil, fmt.Errorf("failed creating temp dir: %w", err)
		}
		h.tempDir = td
	} else if cfg.DataDir != "" {
		h.tempDir = cfg.DataDir
	}

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup peer addresses
	if !cfg.UseTCP {
		for i := 0; i < count; i++ {
			addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("127.0.0.1:3000%d", i+1)))
			h.transports[i] = trans
			h.peers[i] = consensus.RaftPeer{
				ID:      fmt.Sprintf("coord-%d", i+1),
				Address: string(addr),
			}
		}
		// Fully mesh in-memory transports
		for i := 0; i < count; i++ {
			for j := 0; j < count; j++ {
				if i != j {
					h.transports[i].Connect(h.transports[j].LocalAddr(), h.transports[j])
				}
			}
		}
		h.Injector = NewFaultInjector(h.transports, journal)
	} else {
		// TCP Mode: allocate TCP ports
		for i := 0; i < count; i++ {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, fmt.Errorf("failed allocating tcp port %d: %w", i+1, err)
			}
			addr := lis.Addr().String()
			_ = lis.Close()
			h.peers[i] = consensus.RaftPeer{
				ID:      fmt.Sprintf("coord-%d", i+1),
				Address: addr,
			}
		}
	}

	// Initialize and boot all nodes
	for i := 0; i < count; i++ {
		if err := h.bootNode(i, silentLogger); err != nil {
			h.Close()
			return nil, err
		}
	}

	return h, nil
}

func (h *ChaosHarness) bootNode(idx int, logger *slog.Logger) error {
	nodeID := fmt.Sprintf("coord-%d", idx+1)
	st := state.NewStore(storage.NewMemoryAuditStore())
	h.stores[idx] = st
	fsm := consensus.NewFSM(st, logger)

	var nodeDataDir string
	if h.Cfg.UseTCP && h.tempDir != "" {
		nodeDataDir = filepath.Join(h.tempDir, nodeID)
	}

	var transport raft.Transport
	var bindAddr string
	if !h.Cfg.UseTCP {
		transport = h.transports[idx]
		bindAddr = string(h.transports[idx].LocalAddr())
	} else {
		bindAddr = h.peers[idx].Address
	}

	cfg := &consensus.NodeConfig{
		NodeID:       nodeID,
		BindAddr:     bindAddr,
		DataDir:      nodeDataDir,
		Bootstrap:    idx == 0,
		InMemory:     !h.Cfg.UseTCP,
		Peers:        h.peers,
		HeartbeatDur: h.Cfg.HeartbeatDur,
		ElectionMin:  h.Cfg.ElectionMin,
		ElectionMax:  h.Cfg.ElectionMax,
	}

	node, err := consensus.NewRaftNode(cfg, fsm, transport, logger)
	if err != nil {
		return fmt.Errorf("failed creating raft node %s: %w", nodeID, err)
	}
	h.nodes[idx] = node

	coordCfg := config.DefaultCoordinatorConfig(nodeID, 0, 0, bindAddr)
	coordCfg.WorkerLeaseDur = h.Cfg.WorkerLease
	coordCfg.LeaseGraceWindow = h.Cfg.WorkerLease / 2
	coordCfg.FailoverReconcile = h.Cfg.ReconcileDur
	coordCfg.ReaperInterval = 50 * time.Millisecond

	srv := coordinator.NewServer(coordCfg, st, &scheduler.FIFOPolicy{}, logger)
	srv.SetRaftNode(node)
	h.servers[idx] = srv

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("failed listening gRPC for %s: %w", nodeID, err)
	}
	h.listeners[idx] = lis
	h.grpcAddrs[idx] = lis.Addr().String()

	go func(s *coordinator.Server, l net.Listener) {
		_ = s.Start(l)
	}(srv, lis)

	conn, err := grpc.NewClient(h.grpcAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed dialing gRPC for %s: %w", nodeID, err)
	}
	h.conns[idx] = conn
	h.clients[idx] = schedulerv1.NewCoordinatorServiceClient(conn)
	h.wClients[idx] = schedulerv1.NewWorkerServiceClient(conn)

	return nil
}

// WaitLeader waits up to timeout for a cluster leader to emerge.
func (h *ChaosHarness) WaitLeader(timeout time.Duration) int {
	start := time.Now()
	for time.Since(start) < timeout {
		for i, n := range h.nodes {
			if n != nil && n.IsLeader() {
				h.Journal.Record(EventLeaderChanged, n.Config().NodeID, "", "", n.CurrentTerm(), n.LastIndex(),
					"Leader active: %s in term %d (took %v)", n.Config().NodeID, n.CurrentTerm(), time.Since(start))
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

// FindLeaderIndex returns current leader index or -1 if none.
func (h *ChaosHarness) FindLeaderIndex() int {
	for i, n := range h.nodes {
		if n != nil && n.IsLeader() {
			return i
		}
	}
	return -1
}

// KillNode shuts down the Raft node and gRPC server for node index.
func (h *ChaosHarness) KillNode(idx int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	nodeID := fmt.Sprintf("coord-%d", idx+1)
	if h.servers[idx] != nil {
		h.servers[idx].Stop()
		h.servers[idx] = nil
	}
	if h.nodes[idx] != nil {
		_ = h.nodes[idx].Shutdown()
		h.nodes[idx] = nil
	}
	if h.conns[idx] != nil {
		_ = h.conns[idx].Close()
		h.conns[idx] = nil
	}
	if !h.Cfg.UseTCP && h.transports[idx] != nil {
		h.transports[idx].DisconnectAll()
	}

	h.Journal.Record(EventNodeKilled, nodeID, "", "", 0, 0,
		"Node %s terminated", nodeID)
}

// RestartNode restarts a previously killed node, recovering prior persistent state if in TCP mode.
func (h *ChaosHarness) RestartNode(idx int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	nodeID := fmt.Sprintf("coord-%d", idx+1)
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st := state.NewStore(storage.NewMemoryAuditStore())
	h.stores[idx] = st
	fsm := consensus.NewFSM(st, silentLogger)

	var nodeDataDir string
	if h.Cfg.UseTCP && h.tempDir != "" {
		nodeDataDir = filepath.Join(h.tempDir, nodeID)
	}

	var transport raft.Transport
	var bindAddr string
	if !h.Cfg.UseTCP {
		// Re-create transport with same address
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("127.0.0.1:3000%d", idx+1)))
		h.transports[idx] = trans
		transport = trans
		bindAddr = string(addr)

		// Reconnect to active transports
		for j, ot := range h.transports {
			if j != idx && ot != nil {
				trans.Connect(ot.LocalAddr(), ot)
				ot.Connect(trans.LocalAddr(), trans)
			}
		}
	} else {
		bindAddr = h.peers[idx].Address
	}

	cfg := &consensus.NodeConfig{
		NodeID:       nodeID,
		BindAddr:     bindAddr,
		DataDir:      nodeDataDir,
		Bootstrap:    false, // Rejoin existing cluster
		InMemory:     !h.Cfg.UseTCP,
		Peers:        h.peers,
		HeartbeatDur: h.Cfg.HeartbeatDur,
		ElectionMin:  h.Cfg.ElectionMin,
		ElectionMax:  h.Cfg.ElectionMax,
	}

	node, err := consensus.NewRaftNode(cfg, fsm, transport, silentLogger)
	if err != nil {
		return fmt.Errorf("failed restarting raft node %s: %w", nodeID, err)
	}
	h.nodes[idx] = node

	coordCfg := config.DefaultCoordinatorConfig(nodeID, 0, 0, bindAddr)
	coordCfg.WorkerLeaseDur = h.Cfg.WorkerLease
	coordCfg.LeaseGraceWindow = h.Cfg.WorkerLease / 2
	coordCfg.FailoverReconcile = h.Cfg.ReconcileDur
	coordCfg.ReaperInterval = 50 * time.Millisecond

	srv := coordinator.NewServer(coordCfg, st, &scheduler.FIFOPolicy{}, silentLogger)
	srv.SetRaftNode(node)
	h.servers[idx] = srv

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("failed listening gRPC for restarted %s: %w", nodeID, err)
	}
	h.listeners[idx] = lis
	h.grpcAddrs[idx] = lis.Addr().String()

	go func(s *coordinator.Server, l net.Listener) {
		_ = s.Start(l)
	}(srv, lis)

	conn, err := grpc.NewClient(h.grpcAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed dialing gRPC for restarted %s: %w", nodeID, err)
	}
	h.conns[idx] = conn
	h.clients[idx] = schedulerv1.NewCoordinatorServiceClient(conn)
	h.wClients[idx] = schedulerv1.NewWorkerServiceClient(conn)

	h.Journal.Record(EventNodeRestarted, nodeID, "", "", node.CurrentTerm(), node.LastIndex(),
		"Node %s restarted and rejoined cluster", nodeID)

	return nil
}

// Client returns CoordinatorServiceClient for node index.
func (h *ChaosHarness) Client(idx int) schedulerv1.CoordinatorServiceClient {
	return h.clients[idx]
}

// WorkerClient returns WorkerServiceClient for node index.
func (h *ChaosHarness) WorkerClient(idx int) schedulerv1.WorkerServiceClient {
	return h.wClients[idx]
}

// Store returns the in-memory store for node index.
func (h *ChaosHarness) Store(idx int) *state.Store {
	return h.stores[idx]
}

// Node returns the RaftNode for node index.
func (h *ChaosHarness) Node(idx int) *consensus.RaftNode {
	return h.nodes[idx]
}

// Server returns the coordinator Server for node index.
func (h *ChaosHarness) Server(idx int) *coordinator.Server {
	return h.servers[idx]
}

// Close gracefully tears down the entire harness and cleans temp directories.
func (h *ChaosHarness) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return
	}
	h.closed = true

	for _, s := range h.servers {
		if s != nil {
			s.Stop()
		}
	}
	for _, n := range h.nodes {
		if n != nil {
			_ = n.Shutdown()
		}
	}
	for _, c := range h.conns {
		if c != nil {
			_ = c.Close()
		}
	}
	for _, l := range h.listeners {
		if l != nil {
			_ = l.Close()
		}
	}
	if h.tempDir != "" {
		_ = os.RemoveAll(h.tempDir)
	}
}

// Helper methods for direct domain actions
func (h *ChaosHarness) SubmitTaskDirect(ctx context.Context, leaderIdx int, task *domain.Task) error {
	cmd := &consensus.Command{
		Op:        consensus.OpSubmitTask,
		Task:      task,
		Timestamp: time.Now(),
	}
	_, err := h.nodes[leaderIdx].Apply(cmd, 3*time.Second)
	if err == nil {
		h.Journal.Record(EventFaultInjected, h.nodes[leaderIdx].Config().NodeID, "", task.ID,
			h.nodes[leaderIdx].CurrentTerm(), h.nodes[leaderIdx].LastIndex(),
			"Task %s submitted directly to leader", task.ID)
	}
	return err
}
