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
}

// Due returns messages whose deadline has passed.
func (t *Tracker) Due(pid int, now time.Time) []*model.Message {
	out := make([]*model.Message, 0)
	kept := make([]*model.Message, 0)
	for _, m := range t.unacked[pid] {
		if now.Sub(m.Timestamp) >= t.deadline {
			out = append(out, m)
		} else {
			kept = append(kept, m)
		}
	}
	t.unacked[pid] = kept
	return out
}
