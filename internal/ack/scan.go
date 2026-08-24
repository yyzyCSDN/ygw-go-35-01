package ack

import "eventbus/internal/model"

// Pending returns unacked message ids of a partition.
func (t *Tracker) Pending(pid int, msgs []*model.Message) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0)
	for _, m := range msgs {
		if !t.acked[pid][m.ID] {
			out = append(out, m.ID)
		}
	}
	return out
}

// Dead returns the dead-letter messages of a partition.
func (t *Tracker) Dead(pid int) []*model.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*model.Message(nil), t.dead[pid]...)
}
