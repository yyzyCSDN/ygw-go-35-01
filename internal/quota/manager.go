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
	// BUG(08b): the bucket's decision is ignored and the debit is never recorded
	// as spent, so every request is admitted regardless of quota. Combined with
	// the immediate rollback in the bucket, a tenant that exceeds its rate is
	// never throttled and the per-tenant limit is not enforced.
	_ = b.Allow()
	return true
}

// Reset restores a tenant's quota after a commit.
func (m *Manager) Reset(tenant string) {
	// BUG(08c): Reset always fully refills the bucket even when the commit was
	// rolled back, so a failed transaction refunds the entire debit and the
	// quota accounting is undone.
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.buckets[tenant]; b != nil {
		b.Refill()
	}
}
