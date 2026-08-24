// Package retention removes old segments while keeping consumer offsets valid.
package retention

import (
	"time"

	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

// Cleaner deletes segments older than the retention window.
type Cleaner struct {
	store   *partition.Store
	offsets *offset.Manager
	now     func() time.Time
}

// New creates a retention cleaner.
func New(s *partition.Store, o *offset.Manager) *Cleaner {
	return &Cleaner{store: s, offsets: o, now: time.Now}
}

// Clean removes expired segments, never dropping segments that still contain
// messages a consumer has not committed past.
func (c *Cleaner) Clean(p *model.Partition, seg string, cutoff time.Time) int {
	msgs := c.store.Read(p.ID, seg, 0)
	if len(msgs) == 0 {
		return 0
	}
	latest := msgs[len(msgs)-1]
	if latest.Timestamp.After(cutoff) {
		return 0
	}
	end := c.store.SegmentEnd(p.ID, seg)
	if end > c.offsets.Committed(p.ID) {
		return 0
	}
	c.store.DropSegment(p.ID, seg)
	return len(msgs)
}
