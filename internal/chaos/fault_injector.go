package chaos

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// FaultInjector coordinates network partitions, link disruptions, and node lifecycle events.
type FaultInjector struct {
	transports []*raft.InmemTransport
	mu         sync.RWMutex
	partition  map[int]int // Maps nodeIndex -> partitionGroupID
	journal    *Journal
}

// NewFaultInjector creates a fault injector for the cluster transports.
func NewFaultInjector(transports []*raft.InmemTransport, journal *Journal) *FaultInjector {
	fi := &FaultInjector{
		transports: transports,
		partition:  make(map[int]int),
		journal:    journal,
	}
	// Initial state: all nodes in group 0 (fully connected)
	for i := range transports {
		fi.partition[i] = 0
	}
	return fi
}

// Partition creates network isolation between groups of node indices.
// Nodes in groupA can communicate with each other; nodes in groupB can communicate with each other.
// Cross-group communication is severed.
func (fi *FaultInjector) Partition(groupA []int, groupB []int) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	for _, a := range groupA {
		fi.partition[a] = 1
	}
	for _, b := range groupB {
		fi.partition[b] = 2
	}

	// Disconnect cross-group links
	for _, a := range groupA {
		for _, b := range groupB {
			if fi.transports[a] != nil && fi.transports[b] != nil {
				fi.transports[a].Disconnect(fi.transports[b].LocalAddr())
				fi.transports[b].Disconnect(fi.transports[a].LocalAddr())
			}
		}
	}

	if fi.journal != nil {
		fi.journal.Record(EventPartitionCreated, "", "", "", 0, 0,
			"Network partition applied: GroupA=%v vs GroupB=%v", groupA, groupB)
	}
}

// IsolateNode completely isolates a single node from all other cluster members.
func (fi *FaultInjector) IsolateNode(nodeIdx int) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	if fi.transports[nodeIdx] != nil {
		fi.transports[nodeIdx].DisconnectAll()
	}

	for i, t := range fi.transports {
		if i != nodeIdx && t != nil && fi.transports[nodeIdx] != nil {
			t.Disconnect(fi.transports[nodeIdx].LocalAddr())
		}
	}

	if fi.journal != nil {
		fi.journal.Record(EventPartitionCreated, fmt.Sprintf("coord-%d", nodeIdx+1), "", "", 0, 0,
			"Node coord-%d completely isolated from cluster", nodeIdx+1)
	}
}

// CutDirectedLink severs communication in one direction: from srcIdx -> dstIdx.
func (fi *FaultInjector) CutDirectedLink(srcIdx, dstIdx int) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	if fi.transports[srcIdx] != nil && fi.transports[dstIdx] != nil {
		fi.transports[srcIdx].Disconnect(fi.transports[dstIdx].LocalAddr())
	}

	if fi.journal != nil {
		fi.journal.Record(EventFaultInjected, fmt.Sprintf("coord-%d", srcIdx+1), "", "", 0, 0,
			"Cut directed link: coord-%d -> coord-%d", srcIdx+1, dstIdx+1)
	}
}

// Heal restores full bidirectional connectivity between all active transports.
func (fi *FaultInjector) Heal() {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	for i := range fi.transports {
		fi.partition[i] = 0
	}

	count := len(fi.transports)
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j && fi.transports[i] != nil && fi.transports[j] != nil {
				fi.transports[i].Connect(fi.transports[j].LocalAddr(), fi.transports[j])
			}
		}
	}

	if fi.journal != nil {
		fi.journal.Record(EventPartitionHealed, "", "", "", 0, 0,
			"All network partitions healed; full mesh restored")
	}
}

// InjectDelay introduces synthetic pause before execution.
func (fi *FaultInjector) InjectDelay(d time.Duration, reason string) {
	if fi.journal != nil {
		fi.journal.Record(EventFaultInjected, "", "", "", 0, 0,
			"Injected network/processing delay: %v (%s)", d, reason)
	}
	time.Sleep(d)
}
