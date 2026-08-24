// Package ack tracks acknowledged messages and dead-letter handling.
package ack

import (
	"sync"

	"eventbus/internal/model"
)

// Tracker records acknowledged message ids per partition.
type Tracker struct {
	mu     sync.Mutex
	acked  map[int]map[string]bool
	dead   map[int][]*model.Message
}

// New creates an ack tracker.
func New() *Tracker {
	return &Tracker{acked: make(map[int]map[string]bool), dead: make(map[int][]*model.Message)}
}

// Ack marks a message as acknowledged.
func (t *Tracker) Ack(pid int, id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.acked[pid] == nil {
		t.acked[pid] = make(map[string]bool)
	}
	t.acked[pid][id] = true
}

// Acked reports whether a message was acknowledged.
func (t *Tracker) Acked(pid int, id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acked[pid][id]
}

// DeadLetter sends a message to the dead-letter list.
func (t *Tracker) DeadLetter(pid int, msg *model.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dead[pid] = append(t.dead[pid], msg)
}
