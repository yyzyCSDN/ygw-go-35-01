// Package compaction rewrites segments keeping only the latest message per key.
package compaction

import (
	"eventbus/internal/partition"
)

// Compactor collapses a segment to the last value per key.
type Compactor struct {
	store *partition.Store
}

// New creates a compactor.
func New(s *partition.Store) *Compactor {
	return &Compactor{store: s}
}

// Compact returns the latest message per key from a segment.
func (c *Compactor) Compact(pid int, seg string) []*MessageView {
	msgs := c.store.Read(pid, seg, 0)
	latest := make(map[string]*MessageView)
	for _, m := range msgs {
		latest[m.Key] = &MessageView{ID: m.ID, Key: m.Key, Value: m.Value, Offset: m.Offset}
	}
	out := make([]*MessageView, 0, len(latest))
	for _, v := range latest {
		out = append(out, v)
	}
	return out
}

// MessageView is a compacted message entry.
type MessageView struct {
	ID     string
	Key    string
	Value  string
	Offset int64
}
