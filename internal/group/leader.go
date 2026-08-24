package group

// Leader is the elected owner of a partition within a group.
type Leader struct {
	Group    string
	Partition int
	Member   string
	Epoch    uint64
}

// Elect returns the lowest-id active member as leader.
func (c *Coordinator) Elect(group string, pid int, epoch uint64) Leader {
	members := c.Members(group)
	if len(members) == 0 {
		return Leader{Group: group, Partition: pid, Epoch: epoch}
	}
	return Leader{Group: group, Partition: pid, Member: members[0], Epoch: epoch}
}
