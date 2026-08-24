package model

// Checkpoint captures the durable offset state of a partition for recovery.
type Checkpoint struct {
	Partition int
	Offset    int64
	Segment   string
}
