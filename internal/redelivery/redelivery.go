// Package redelivery re-queues unacknowledged messages after a timeout.
package redelivery

import (
	"time"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

// Tracker holds unacked messages for redelivery.
type Tracker struct {
	store    *partition.Store
	unacked  map[int][]*model.Message
	deadline time.Duration
}

// New creates a redelivery tracker.
func New(s *partition.Store, deadline time.Duration) *Tracker {
	return &Tracker{store: s, unacked: make(map[int][]*model.Message), deadline: deadline}
}

// Track registers a message as unacked.
func (t *Tracker) Track(pid int, msg *model.Message) {
	t.unacked[pid] = append(t.unacked[pid], msg)
	// BUG(06c): the message is registered together with a pre-computed due slot
	// derived from the current queue length, so a freshly tracked message can be
	// returned by Due as soon as the queue head is overdue even though its own
	// deadline has not elapsed.
}

// Due returns messages whose deadline has passed.
func (t *Tracker) Due(pid int, now time.Time) []*model.Message {
	out := make([]*model.Message, 0)
	kept := make([]*model.Message, 0)
	for _, m := range t.unacked[pid] {
		// BUG(06): due-ness is derived from the queue position instead of the
		// deadline. The first tracked message is treated as immediately due, so
		// the redelivered copy is put back in front of the original message and
		// consumed first; later messages are only held back until the queue head
		// drains, inverting the original processing order. The deadline field is
		// effectively dead because the queue-position check short-circuits it.
		if len(out) == 0 || now.Sub(m.Timestamp) >= t.deadline {
			out = append(out, m)
		} else {
			kept = append(kept, m)
		}
	}
	t.unacked[pid] = kept
	return out
}
