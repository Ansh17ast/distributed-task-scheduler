package coordinator

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterHarness manages an in-memory replicated Raft cluster with gRPC servers and clients.
type ClusterHarness struct {
	Count      int
	Transports []*raft.InmemTransport
	Nodes      []*consensus.RaftNode
	Servers    []*Server
	Stores     []*state.Store
	Listeners  []net.Listener
	GrpcAddrs  []string
	Clients    []schedulerv1.CoordinatorServiceClient
	WClients   []schedulerv1.WorkerServiceClient
	Conns      []*grpc.ClientConn
}

// NewClusterHarness bootstraps an in-process, fully meshed N-node cluster.
func NewClusterHarness(count int) (*ClusterHarness, error) {
	h := &ClusterHarness{
		Count:      count,
		Transports: make([]*raft.InmemTransport, count),
		Nodes:      make([]*consensus.RaftNode, count),
		Servers:    make([]*Server, count),
		Stores:     make([]*state.Store, count),
		Listeners:  make([]net.Listener, count),
		GrpcAddrs:  make([]string, count),
		Clients:    make([]schedulerv1.CoordinatorServiceClient, count),
		WClients:   make([]schedulerv1.WorkerServiceClient, count),
		Conns:      make([]*grpc.ClientConn, count),
	}

	peers := make([]consensus.RaftPeer, count)
	for i := 0; i < count; i++ {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("127.0.0.1:2500%d", i+1)))
		h.Transports[i] = trans
		peers[i] = consensus.RaftPeer{
			ID:      fmt.Sprintf("coord-%d", i+1),
			Address: string(addr),
		}
	}

	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				h.Transports[i].Connect(h.Transports[j].LocalAddr(), h.Transports[j])
			}
		}
	}

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < count; i++ {
		store := state.NewStore(storage.NewMemoryAuditStore())
		h.Stores[i] = store
		fsm := consensus.NewFSM(store, silentLogger)

		cfg := &consensus.NodeConfig{
			NodeID:       fmt.Sprintf("coord-%d", i+1),
			BindAddr:     string(h.Transports[i].LocalAddr()),
			Bootstrap:    i == 0,
			InMemory:     true,
			Peers:        peers,
			HeartbeatDur: 25 * time.Millisecond,
			ElectionMin:  75 * time.Millisecond,
			ElectionMax:  150 * time.Millisecond,
		}

		node, err := consensus.NewRaftNode(cfg, fsm, h.Transports[i], silentLogger)
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("failed creating raft node %d: %w", i+1, err)
		}
		h.Nodes[i] = node

		coordCfg := config.DefaultCoordinatorConfig(fmt.Sprintf("coord-%d", i+1), 0, 0, cfg.BindAddr)
		coordCfg.WorkerLeaseDur = 2 * time.Second
		coordCfg.LeaseGraceWindow = 1 * time.Second
		coordCfg.FailoverReconcile = 1 * time.Second
		coordCfg.ReaperInterval = 100 * time.Millisecond

		server := NewServer(coordCfg, store, &scheduler.FIFOPolicy{}, silentLogger)
		server.SetRaftNode(node)
		h.Servers[i] = server

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("failed listening gRPC for server %d: %w", i+1, err)
		}
		h.Listeners[i] = lis
		h.GrpcAddrs[i] = lis.Addr().String()

		go func(s *Server, l net.Listener) {
			_ = s.Start(l)
		}(server, lis)

		conn, err := grpc.NewClient(h.GrpcAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("failed dialing gRPC for server %d: %w", i+1, err)
		}
		h.Conns[i] = conn
		h.Clients[i] = schedulerv1.NewCoordinatorServiceClient(conn)
		h.WClients[i] = schedulerv1.NewWorkerServiceClient(conn)
	}

	return h, nil
}

// Close terminates all servers, listeners, and gRPC client connections.
func (h *ClusterHarness) Close() {
	for i := 0; i < h.Count; i++ {
		if h.Conns[i] != nil {
			_ = h.Conns[i].Close()
		}
		if h.Servers[i] != nil {
			h.Servers[i].Stop()
		}
		if h.Listeners[i] != nil {
			_ = h.Listeners[i].Close()
		}
	}
}

// FindLeaderIndex returns the index of the current Raft leader or -1.
func (h *ClusterHarness) FindLeaderIndex() int {
	for i := 0; i < h.Count; i++ {
		if h.Nodes[i] != nil && h.Nodes[i].IsLeader() {
			return i
		}
	}
	return -1
}

// WaitLeader blocks until a leader is elected or timeout expires.
func (h *ClusterHarness) WaitLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx := h.FindLeaderIndex()
		if idx >= 0 {
			return idx, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, fmt.Errorf("timeout waiting for leader election")
}
