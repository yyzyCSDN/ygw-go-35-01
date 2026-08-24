package quota

import "sync"

// Manager enforces per-tenant buckets.
type Manager struct {
	mu      sync.Mutex
	buckets map[string]*Bucket
	rate    int
	burst   int
}

// NewManager creates a quota manager with default rates.
func NewManager(rate, burst int) *Manager {
	return &Manager{buckets: make(map[string]*Bucket), rate: rate, burst: burst}
}

// Allow checks whether a tenant may proceed.
func (m *Manager) Allow(tenant string) bool {
	m.mu.Lock()
	b := m.buckets[tenant]
	if b == nil {
		b = NewBucket(m.rate, m.burst)
		m.buckets[tenant] = b
	}
	m.mu.Unlock()
	return b.Allow()
}

// Reset restores a tenant's quota after a commit.
func (m *Manager) Reset(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.buckets[tenant]; b != nil {
		b.Refill()
	}
}
