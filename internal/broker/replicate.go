package broker

import "sync"

// AckPolicy describes how many replicas must acknowledge an append.
type AckPolicy struct {
	MinAcks    int
	WaitLeader bool
}

// DefaultAckPolicy requires a single ack from the leader.
func DefaultAckPolicy() AckPolicy {
	return AckPolicy{MinAcks: 1, WaitLeader: true}
}

// Replicator tracks per-partition acknowledge counts.
type Replicator struct {
	mu    sync.Mutex
	acks  map[int]int
	ready map[int]bool
}

// NewReplicator creates a replicator.
func NewReplicator() *Replicator {
	return &Replicator{acks: make(map[int]int), ready: make(map[int]bool)}
}

// Ack records one replica ack for a partition.
func (r *Replicator) Ack(pid int, policy AckPolicy) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks[pid]++
	if r.acks[pid] >= policy.MinAcks {
		r.ready[pid] = true
	}
	return r.ready[pid]
}

// Ready reports whether a partition reached its ack threshold.
func (r *Replicator) Ready(pid int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready[pid]
}
