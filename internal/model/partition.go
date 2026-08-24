package model

import "sync/atomic"

// PartitionState is the lifecycle state of a partition.
type PartitionState int

const (
	// PartitionActive accepts appends and reads.
	PartitionActive PartitionState = iota
	// PartitionSealed stops accepting appends.
	PartitionSealed
	// PartitionRetired is being cleaned and removed.
	PartitionRetired
)

func (s PartitionState) String() string {
	switch s {
	case PartitionActive:
		return "active"
	case PartitionSealed:
		return "sealed"
	case PartitionRetired:
		return "retired"
	default:
		return "unknown"
	}
}

// Partition is an ordered message log.
type Partition struct {
	Topic      string
	ID         int
	NextOffset int64
	state      atomic.Int32
}

// NewPartition creates an active partition.
func NewPartition(topic string, id int) *Partition {
	p := &Partition{Topic: topic, ID: id}
	p.state.Store(int32(PartitionActive))
	return p
}

// State returns the partition lifecycle state.
func (p *Partition) State() PartitionState {
	return PartitionState(p.state.Load())
}

// SetState transitions the partition state.
func (p *Partition) SetState(s PartitionState) {
	p.state.Store(int32(s))
}
