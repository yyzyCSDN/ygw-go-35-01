package group

import "sort"

// Assignment is an immutable snapshot of a group's partition distribution.
type Assignment struct {
	Group    string
	Version  uint64
	ByMember map[string][]int
}

// CurrentAssignment returns the latest assignment snapshot.
func (c *Coordinator) CurrentAssignment(group string) Assignment {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	out := Assignment{Group: group, ByMember: make(map[string][]int)}
	if g == nil {
		return out
	}
	out.Version = g.Version
	for id, m := range g.Members {
		out.ByMember[id] = append([]int(nil), m.Partitions...)
	}
	for _, ps := range out.ByMember {
		sort.Ints(ps)
	}
	return out
}
