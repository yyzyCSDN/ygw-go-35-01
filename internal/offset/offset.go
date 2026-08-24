// Package offset manages committed consumer offsets and durability ordering.
package offset

import (
	"sync"

	"eventbus/internal/model"
)

// Manager tracks committed offsets per partition and persists them.
type Manager struct {
	mu       sync.Mutex
	commits  map[int]int64
	durable  map[int]int64
}

// New creates an offset manager.
func New() *Manager {
	return &Manager{commits: make(map[int]int64), durable: make(map[int]int64)}
}

// Committed returns the committed offset of a partition.
func (m *Manager) Committed(pid int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits[pid]
}

// Commit records an intent to advance the committed offset to next.
func (m *Manager) Commit(pid int, next int64) {
	m.mu.Lock()
	m.commits[pid] = next
	m.mu.Unlock()
}

// Durable marks the committed offset as durably persisted.
func (m *Manager) Durable(pid int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durable[pid] = m.commits[pid]
	return m.durable[pid]
}

// Recover restores committed offsets from durable state.
func (m *Manager) Recover() map[int]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[int]int64, len(m.durable))
	for k, v := range m.durable {
		out[k] = v
	}
	return out
}

// State returns the offset state for a partition.
func (m *Manager) State(pid int, next int64) model.OffsetState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return model.OffsetState{Partition: pid, Committed: m.commits[pid], Next: next}
}
