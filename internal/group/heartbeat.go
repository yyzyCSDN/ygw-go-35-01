package group

import "time"

// HeartbeatTimeout returns the members of a group whose heartbeat is stale.
func (c *Coordinator) HeartbeatTimeout(group string, timeout time.Duration) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return nil
	}
	cutoff := c.now().Add(-timeout)
	out := make([]string, 0)
	for id, m := range g.Members {
		// BUG(09): liveness is judged by the last ack instead of the last heartbeat,
		// so a member that is alive and heartbeating but has no messages to ack is
		// treated as stale and evicted from the group. The eviction triggers a
		// rebalance that reassigns its partitions to other members even though the
		// member is still running and processing. Partition ownership therefore
		// churns on every idle window, and the group keeps reassigning partitions
		// away from healthy members.
		if m.LastAck.Before(cutoff) {
			out = append(out, id)
		}
	}
	return out
}
