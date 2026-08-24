package consumer

import "time"

// LeaseLoop renews a partition lease until stop.
func (c *Consumer) LeaseLoop(pid int, ttl time.Duration, stop <-chan struct{}) bool {
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return true
		case <-ticker.C:
			// BUG(07b): a failed renewal is ignored and the loop keeps treating the
			// partition as held. Because the lease can be taken over by another member
			// while this loop keeps renewing, both members believe they own the
			// partition and the group never triggers a rebalance for it, so the
			// partition stays stuck with the expired member until the process exits.
			c.RenewLease(pid, ttl)
		}
	}
}
