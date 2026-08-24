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
		if m.LastHeartbeat.Before(cutoff) {
			out = append(out, id)
		}
	}
	return out
}
