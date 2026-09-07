package config

import (
	"errors"
	"fmt"
	"time"
)

// CoordinatorConfig defines runtime configuration for a coordinator node.
type CoordinatorConfig struct {
	NodeID            string        `json:"node_id"`
	GRPCPort          int           `json:"grpc_port"`
	HTTPPort          int           `json:"http_port"`
	RaftBindAddr      string        `json:"raft_bind_addr"`
	RaftDataDir       string        `json:"raft_data_dir"`
	RaftPeers         []string      `json:"raft_peers"`
	RaftBootstrap     bool          `json:"raft_bootstrap"`
	RaftHeartbeat     time.Duration `json:"raft_heartbeat"`
	RaftElectionMin   time.Duration `json:"raft_election_min"`
	RaftElectionMax   time.Duration `json:"raft_election_max"`
	WorkerLeaseDur    time.Duration `json:"worker_lease_duration"`
	LeaseGraceWindow  time.Duration `json:"lease_grace_window"`
	FailoverReconcile time.Duration `json:"failover_reconcile"`
	ReaperInterval    time.Duration `json:"reaper_interval"`
	LogLevel          string        `json:"log_level"`
}

// DefaultCoordinatorConfig returns baseline configurations for benchmark testing.
func DefaultCoordinatorConfig(nodeID string, grpcPort, httpPort int, raftBind string) *CoordinatorConfig {
	return &CoordinatorConfig{
		NodeID:            nodeID,
		GRPCPort:          grpcPort,
		HTTPPort:          httpPort,
		RaftBindAddr:      raftBind,
		RaftDataDir:       fmt.Sprintf("./data/raft-%s", nodeID),
		RaftPeers:         []string{},
		RaftBootstrap:     false,
		RaftHeartbeat:     50 * time.Millisecond,
		RaftElectionMin:   150 * time.Millisecond,
		RaftElectionMax:   300 * time.Millisecond,
		WorkerLeaseDur:    10 * time.Second,
		LeaseGraceWindow:  2 * time.Second,
		FailoverReconcile: 4 * time.Second,
		ReaperInterval:    1 * time.Second,
		LogLevel:          "INFO",
	}
}

// Validate checks for logical timing violations in the configuration.
func (c *CoordinatorConfig) Validate() error {
	if c.NodeID == "" {
		return errors.New("coordinator node_id cannot be empty")
	}
	if c.GRPCPort <= 0 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid grpc_port: %d", c.GRPCPort)
	}
	if c.RaftHeartbeat <= 0 {
		return errors.New("raft_heartbeat must be positive")
	}
	if c.RaftElectionMin <= c.RaftHeartbeat {
		return errors.New("raft_election_min must be greater than raft_heartbeat")
	}
	if c.RaftElectionMax <= c.RaftElectionMin {
		return errors.New("raft_election_max must be greater than raft_election_min")
	}
	if c.WorkerLeaseDur <= 0 {
		return errors.New("worker_lease_duration must be positive")
	}
	return nil
}

// WorkerConfig defines runtime configuration for a worker daemon.
type WorkerConfig struct {
	WorkerID            string        `json:"worker_id"`
	SessionID           string        `json:"session_id,omitempty"` // Unique process incarnation UUID
	Address             string        `json:"address"`
	CoordinatorEndpoint string        `json:"coordinator_endpoint"`
	CoordinatorAddrs    []string      `json:"coordinator_addrs,omitempty"` // Cluster coordinator endpoints for failover
	MaxSlots            int32         `json:"max_slots"`
	HeartbeatInterval   time.Duration `json:"heartbeat_interval"`
	PreemptBuffer       time.Duration `json:"preempt_buffer"`
	LogLevel            string        `json:"log_level"`
}

// DefaultWorkerConfig returns baseline configurations for a worker node.
func DefaultWorkerConfig(workerID string, coordinatorEndpoint string) *WorkerConfig {
	return &WorkerConfig{
		WorkerID:            workerID,
		Address:             "127.0.0.1:0",
		CoordinatorEndpoint: coordinatorEndpoint,
		MaxSlots:            4,
		HeartbeatInterval:   3 * time.Second,
		PreemptBuffer:       1500 * time.Millisecond,
		LogLevel:            "INFO",
	}
}

// Validate verifies worker configuration validity.
func (w *WorkerConfig) Validate() error {
	if w.WorkerID == "" {
		return errors.New("worker_id cannot be empty")
	}
	if w.CoordinatorEndpoint == "" {
		return errors.New("coordinator_endpoint cannot be empty")
	}
	if w.MaxSlots <= 0 {
		return errors.New("max_slots must be at least 1")
	}
	if w.HeartbeatInterval <= 0 {
		return errors.New("heartbeat_interval must be positive")
	}
	return nil
}
