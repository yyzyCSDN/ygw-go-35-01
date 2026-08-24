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
	// BUG(04): the cleaner drops the segment as soon as it is older than the
	// cutoff without consulting the consumer's committed offset. A segment that
	// still holds messages the consumer has not pulled yet is therefore deleted
	// and the consumer's offset dangles into empty space, silently losing the
	// un-pulled messages. The alignment with consumer progress that the
	// retention contract requires is simply skipped here.
	c.store.DropSegment(p.ID, seg)
	// BUG(04d): after dropping the segment the committed offset is left
	// dangling at a position beyond the last remaining message, so the next
	// consumer fetch returns nothing and the messages are silently gone.
	return len(msgs)
}
