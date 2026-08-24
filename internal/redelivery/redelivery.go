// Package redelivery re-queues unacknowledged messages after a timeout.
package redelivery

import (
	"time"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

// pending pairs an unacked message with the absolute time at which it becomes
// eligible for redelivery. Capturing the due time when the message is tracked
// lets Due judge each message by its own deadline, independent of the order or
// length of the in-flight queue, so a redelivered copy can never overtake an
// original whose deadline has not yet elapsed.
type pending struct {
	msg *model.Message
	due time.Time
}

// Tracker holds unacked messages for redelivery.
type Tracker struct {
	store    *partition.Store
	unacked  map[int][]pending
	deadline time.Duration
}

// New creates a redelivery tracker.
func New(s *partition.Store, deadline time.Duration) *Tracker {
	return &Tracker{store: s, unacked: make(map[int][]pending), deadline: deadline}
}

// Track registers a message as unacked, scheduling its redelivery for one
// deadline interval from now. The due time is fixed at registration, so every
// message is judged against its own deadline rather than against the state of
// the in-flight queue.
func (t *Tracker) Track(pid int, msg *model.Message) {
	due := time.Now().Add(t.deadline)
	t.unacked[pid] = append(t.unacked[pid], pending{msg: msg, due: due})
}

// Due returns messages whose individual deadline has passed. Because each
// message carries its own due time, a message is returned only when its own
// deadline elapses — never merely because it happens to be at the head of the
// queue. A redelivered copy therefore cannot be produced before the original
// message's deadline, preserving the original processing order.
func (t *Tracker) Due(pid int, now time.Time) []*model.Message {
	out := make([]*model.Message, 0)
	kept := make([]pending, 0)
	for _, p := range t.unacked[pid] {
		if !now.Before(p.due) {
			out = append(out, p.msg)
		} else {
			kept = append(kept, p)
		}
	}
	t.unacked[pid] = kept
	return out
}
