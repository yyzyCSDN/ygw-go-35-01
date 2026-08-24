package broker

import "sync"

// Metrics aggregates broker-level counters.
type Metrics struct {
	mu      sync.Mutex
	appends int
	deletes int
	errors  int
}

// NewMetrics creates a broker metrics tracker.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncAppend records one append.
func (m *Metrics) IncAppend() {
	m.mu.Lock()
	m.appends++
	m.mu.Unlock()
}

// IncDelete records one topic delete.
func (m *Metrics) IncDelete() {
	m.mu.Lock()
	m.deletes++
	m.mu.Unlock()
}

// IncError records one error.
func (m *Metrics) IncError() {
	m.mu.Lock()
	m.errors++
	m.mu.Unlock()
}

// Appends returns the append count.
func (m *Metrics) Appends() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appends
}
