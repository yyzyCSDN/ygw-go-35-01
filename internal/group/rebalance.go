package group

import "sort"

// Rebalance reassigns partitions to the current members round-robin and
// returns a mapping of member -> partition ids.
//
// The member set is rebuilt from the live membership table on every call so the
// round-robin always reflects the members that are actually in the group right
// now. A member that left (via Remove) is therefore dropped, and a member that
// just joined is included, before partitions are handed out. The fresh
// assignment is then written back to every current member — clearing the stale
// partitions of members that no longer own any — so both the returned mapping
// and CurrentAssignment/Snapshot stay consistent with the latest rebalance.
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
	for id, m := range g.Members {
		m.Partitions = append([]int(nil), assign[id]...)
	}
	g.Version++
	return assign
}
