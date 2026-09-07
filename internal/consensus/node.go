package consensus

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"distributed-scheduler/internal/config"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

var (
	ErrNotLeader = errors.New("node is not cluster leader")
)

// NodeConfig specifies how Raft should be configured for a coordinator node.
type NodeConfig struct {
	NodeID       string
	BindAddr     string
	DataDir      string
	Bootstrap    bool
	InMemory     bool
	Peers        []RaftPeer
	HeartbeatDur time.Duration
	ElectionMin  time.Duration
	ElectionMax  time.Duration
}

// RaftPeer defines a member in the Raft consensus group.
type RaftPeer struct {
	ID      string
	Address string
}

// RaftNode wraps a hashicorp/raft instance and coordinates consensus operations.
type RaftNode struct {
	cfg        *NodeConfig
	raft       *raft.Raft
	fsm        *FSM
	transport  raft.Transport
	boltStore  *raftboltdb.BoltStore
	inmemStore *raft.InmemStore
	logger     *slog.Logger

	leaderCh <-chan bool
	stopCh   chan struct{}
	mu       sync.RWMutex
}

// NewRaftNode initializes and boots a RaftNode.
func NewRaftNode(cfg *NodeConfig, fsm *FSM, transport raft.Transport, logger *slog.Logger) (*RaftNode, error) {
	if logger == nil {
		logger = slog.Default()
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	if cfg.HeartbeatDur > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatDur
	}
	if cfg.ElectionMin > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionMin
	}
	raftConfig.LeaderLeaseTimeout = raftConfig.HeartbeatTimeout
	raftConfig.CommitTimeout = 10 * time.Millisecond

	// Discard internal verbose hashicorp raft logs or redirect
	raftConfig.LogOutput = io.Discard

	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapStore raft.SnapshotStore
	var boltStore *raftboltdb.BoltStore
	var inmemStore *raft.InmemStore

	if cfg.InMemory {
		inmemStore = raft.NewInmemStore()
		logStore = inmemStore
		stableStore = inmemStore
		snapStore = raft.NewInmemSnapshotStore()
	} else {
		// Persistent on-disk storage with BoltDB and FileSnapshotStore
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create raft data directory %s: %w", cfg.DataDir, err)
		}
		dbPath := filepath.Join(cfg.DataDir, "raft.db")
		bs, err := raftboltdb.NewBoltStore(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open raft bolt store at %s: %w", dbPath, err)
		}
		boltStore = bs
		logStore = bs
		stableStore = bs

		fss, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, io.Discard)
		if err != nil {
			_ = bs.Close()
			return nil, fmt.Errorf("failed to initialize file snapshot store: %w", err)
		}
		snapStore = fss
	}

	// If transport not supplied, create standard TCP transport
	if transport == nil {
		addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
		if err != nil {
			if boltStore != nil {
				_ = boltStore.Close()
			}
			return nil, fmt.Errorf("failed to resolve raft bind addr %s: %w", cfg.BindAddr, err)
		}
		tcpTrans, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, io.Discard)
		if err != nil {
			if boltStore != nil {
				_ = boltStore.Close()
			}
			return nil, fmt.Errorf("failed to create raft tcp transport: %w", err)
		}
		transport = tcpTrans
	}

	// Bootstrap cluster if explicitly configured and no previous state exists
	hasExistingState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing raft state: %w", err)
	}

	if cfg.Bootstrap && !hasExistingState {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.Address),
			})
		}
		clusterConfig := raft.Configuration{Servers: servers}
		if err := raft.BootstrapCluster(raftConfig, logStore, stableStore, snapStore, transport, clusterConfig); err != nil {
			if boltStore != nil {
				_ = boltStore.Close()
			}
			return nil, fmt.Errorf("failed to bootstrap raft cluster: %w", err)
		}
		logger.Info("raft cluster bootstrapped", "node_id", cfg.NodeID, "servers", len(servers))
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		if boltStore != nil {
			_ = boltStore.Close()
		}
		return nil, fmt.Errorf("failed to initialize raft: %w", err)
	}

	node := &RaftNode{
		cfg:        cfg,
		raft:       r,
		fsm:        fsm,
		transport:  transport,
		boltStore:  boltStore,
		inmemStore: inmemStore,
		logger:     logger,
		leaderCh:   r.LeaderCh(),
		stopCh:     make(chan struct{}),
	}

	return node, nil
}

// Apply proposes an authoritative mutation Command through Raft consensus.
func (n *RaftNode) Apply(cmd *Command, timeout time.Duration) (interface{}, error) {
	if !n.IsLeader() {
		return nil, ErrNotLeader
	}

	data, err := cmd.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode command: %w", err)
	}

	applyFuture := n.raft.Apply(data, timeout)
	if err := applyFuture.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, ErrNotLeader
		}
		return nil, fmt.Errorf("raft apply error: %w", err)
	}

	resp := applyFuture.Response()
	if applyErr, ok := resp.(error); ok {
		return nil, applyErr
	}
	return resp, nil
}

// VerifyLeader ensures the local node still holds leadership and quorum with peers.
func (n *RaftNode) VerifyLeader() error {
	future := n.raft.VerifyLeader()
	return future.Error()
}

// IsLeader returns true if the local node is currently in the Leader state.
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// State returns the current Raft state (Leader, Follower, Candidate, Shutdown).
func (n *RaftNode) State() raft.RaftState {
	return n.raft.State()
}

// LeaderWithID returns the current leader address and server ID according to Raft.
func (n *RaftNode) LeaderWithID() (raft.ServerAddress, raft.ServerID) {
	return n.raft.LeaderWithID()
}

// LeaderCh returns the channel that notifies on leadership changes.
func (n *RaftNode) LeaderCh() <-chan bool {
	return n.leaderCh
}

// CurrentTerm returns the current consensus term.
func (n *RaftNode) CurrentTerm() uint64 {
	return n.raft.LastIndex()
}

// LastIndex returns the last log index in the Raft log.
func (n *RaftNode) LastIndex() uint64 {
	return n.raft.LastIndex()
}

// NodeID returns the configured node identifier.
func (n *RaftNode) NodeID() string {
	if n.cfg != nil {
		return n.cfg.NodeID
	}
	return ""
}

// Config returns the underlying NodeConfig.
func (n *RaftNode) Config() *NodeConfig {
	return n.cfg
}

// Raft returns the underlying *raft.Raft instance for advanced test controls.
func (n *RaftNode) Raft() *raft.Raft {
	return n.raft
}

// Transport returns the underlying transport.
func (n *RaftNode) Transport() raft.Transport {
	return n.transport
}

// Shutdown gracefully shuts down the Raft node.
func (n *RaftNode) Shutdown() error {
	future := n.raft.Shutdown()
	err := future.Error()

	if n.boltStore != nil {
		_ = n.boltStore.Close()
	}
	if closer, ok := n.transport.(io.Closer); ok {
		_ = closer.Close()
	}
	return err
}

// NodeConfigFromCoordinatorConfig builds a NodeConfig from CoordinatorConfig.
func NodeConfigFromCoordinatorConfig(c *config.CoordinatorConfig) *NodeConfig {
	peers := make([]RaftPeer, 0, len(c.RaftPeers))
	for _, p := range c.RaftPeers {
		peers = append(peers, RaftPeer{
			ID:      p,
			Address: p,
		})
	}
	return &NodeConfig{
		NodeID:       c.NodeID,
		BindAddr:     c.RaftBindAddr,
		DataDir:      c.RaftDataDir,
		Bootstrap:    c.RaftBootstrap,
		InMemory:     false,
		Peers:        peers,
		HeartbeatDur: c.RaftHeartbeat,
		ElectionMin:  c.RaftElectionMin,
		ElectionMax:  c.RaftElectionMax,
	}
}
