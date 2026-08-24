package group

import "sort"

// Rebalance reassigns partitions to the current members round-robin and
// returns a mapping of member -> partition ids.
func (c *Coordinator) Rebalance(group string, partitions []int) map[string][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil || len(g.Members) == 0 {
		return map[string][]int{}
	}
	members := make([]string, 0, len(g.Members))
	for id := range g.Members {
		members = append(members, id)
	}
	sort.Strings(members)
	sorted := append([]int(nil), partitions...)
	sort.Ints(sorted)
	assign := make(map[string][]int)
	for i, p := range sorted {
		m := members[i%len(members)]
		assign[m] = append(assign[m], p)
	}
	for id, ps := range assign {
		if m := g.Members[id]; m != nil {
			m.Partitions = ps
		}
	}
	g.Version++
	return assign
}
