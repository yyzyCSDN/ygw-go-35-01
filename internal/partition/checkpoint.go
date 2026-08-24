package partition

import "eventbus/internal/model"

// Checkpoint captures a partition's durable state for recovery.
type Checkpoint struct {
	Partition int
	NextOffset int64
	Segments   int
	State      string
}

// Capture builds a checkpoint for a partition.
func Capture(p *model.Partition, segCount int) Checkpoint {
	return Checkpoint{
		Partition:  p.ID,
		NextOffset: p.NextOffset,
		Segments:   segCount,
		State:      p.State().String(),
	}
}
