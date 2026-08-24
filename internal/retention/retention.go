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

// Clean removes an expired segment, but only once the consumer group has
// committed past it. A segment that still holds messages a consumer has not
// yet pulled is retained so the committed offset never dangles into space and
// messages are never silently lost.
func (c *Cleaner) Clean(p *model.Partition, seg string, cutoff time.Time) int {
	msgs := c.store.Read(p.ID, seg, 0)
	if len(msgs) == 0 {
		return 0
	}
	latest := msgs[len(msgs)-1]
	if latest.Timestamp.After(cutoff) {
		return 0
	}
	// Align with consumer progress before deleting: drop the segment only when
	// the consumer's committed offset has reached or passed the segment's end.
	// end is the next offset after the last message in this segment, i.e. the
	// offset a consumer must have committed to have consumed every message here.
	end := c.store.SegmentEnd(p.ID, seg)
	committed := c.offsets.Committed(p.ID)
	if committed < end {
		// The consumer still needs messages from this segment, so keep it even
		// though it is older than the retention cutoff.
		return 0
	}
	c.store.DropSegment(p.ID, seg)
	return len(msgs)
}
