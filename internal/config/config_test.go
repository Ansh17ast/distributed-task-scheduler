package config

import (
	"testing"
	"time"
)

func TestCoordinatorConfig_Validate(t *testing.T) {
	cfg := DefaultCoordinatorConfig("coord-1", 9001, 8001, "127.0.0.1:7001")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid default config, got %v", err)
	}

	// Empty node ID
	cfg.NodeID = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for empty node_id, got nil")
	}

	// Invalid election bounds
	cfg = DefaultCoordinatorConfig("coord-1", 9001, 8001, "127.0.0.1:7001")
	cfg.RaftElectionMin = 10 * time.Millisecond
	cfg.RaftHeartbeat = 50 * time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error when election_min <= heartbeat, got nil")
	}
}

func TestWorkerConfig_Validate(t *testing.T) {
	cfg := DefaultWorkerConfig("worker-1", "127.0.0.1:9001")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid default worker config, got %v", err)
	}

	cfg.MaxSlots = 0
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for max_slots = 0, got nil")
	}

	cfg.WorkerID = ""
	if err := cfg.Validate(); err == nil {
		t.Errorf("expected error for empty worker_id, got nil")
	}
}
