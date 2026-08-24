// Package worker runs background maintenance: retention, compaction and
// lease/group reconciliation.
package worker

import (
	"time"

	"eventbus/internal/broker"
	"eventbus/internal/compaction"
	"eventbus/internal/group"
	"eventbus/internal/retention"
)

// Runner executes periodic maintenance tasks.
type Runner struct {
	broker  *broker.Broker
	cleaner *retention.Cleaner
	compact *compaction.Compactor
	groups  *group.Coordinator
	now     func() time.Time
}

// New creates a maintenance runner.
func New(b *broker.Broker, c *retention.Cleaner, cp *compaction.Compactor, g *group.Coordinator) *Runner {
	return &Runner{broker: b, cleaner: c, compact: cp, groups: g, now: time.Now}
}

// RunOnce performs one pass of retention over all topics.
func (r *Runner) RunOnce(cutoff time.Time) int {
	removed := 0
	for _, name := range r.broker.Topics() {
		t, ok := r.broker.Topic(name)
		if !ok {
			continue
		}
		for _, pid := range t.Partitions {
			p := r.broker.Partition(pid)
			if p == nil {
				continue
			}
			for _, seg := range r.broker.Segments(pid) {
				removed += r.cleaner.Clean(p, seg, cutoff)
			}
		}
	}
	return removed
}
