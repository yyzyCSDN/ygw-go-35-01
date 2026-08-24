package group

// MemberState is a snapshot of one member's assignment.
type MemberState struct {
	ID         string
	Partitions []int
	Active     bool
}

// Snapshot returns member states of a group.
func (c *Coordinator) Snapshot(group string) []MemberState {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return nil
	}
	out := make([]MemberState, 0, len(g.Members))
	for id, m := range g.Members {
		out = append(out, MemberState{ID: id, Partitions: append([]int(nil), m.Partitions...), Active: true})
	}
	return out
}
