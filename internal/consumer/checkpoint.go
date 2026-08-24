package consumer

// Checkpoint records the durable position of a consumer group member.
type Checkpoint struct {
	Group    string
	Member   string
	Partition int
	Offset   int64
}

// Snapshot builds a checkpoint for the current committed offset.
func (c *Consumer) Snapshot(pid int) Checkpoint {
	return Checkpoint{
		Group:     c.groupID,
		Member:    c.memberID,
		Partition: pid,
		Offset:    c.offsets.Committed(pid),
	}
}

// Restore applies a checkpoint's offset.
func (c *Consumer) Restore(cp Checkpoint) {
	c.offsets.Commit(cp.Partition, cp.Offset)
	c.offsets.Durable(cp.Partition)
}

// Progress is a lightweight view of group progress.
type Progress struct {
	Group    string
	Member   string
	Committed int64
}

// Progress returns the consumer's committed progress for a partition.
func (c *Consumer) Progress(pid int) Progress {
	return Progress{Group: c.groupID, Member: c.memberID, Committed: c.offsets.Committed(pid)}
}
