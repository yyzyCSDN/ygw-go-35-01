package offset

import "sync"

// Sync coordinates commit durability with message visibility.
type Sync struct {
	mu      sync.Mutex
	pending map[int]int64
	visible map[int]int64
}

// NewSync creates a commit/visibility sync helper.
func NewSync() *Sync {
	return &Sync{pending: make(map[int]int64), visible: make(map[int]int64)}
}

// MarkPending records a commit intent before durability.
func (s *Sync) MarkPending(pid int, next int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[pid] = next
	// BUG(02b): the pending intent is immediately treated as visible, so the
	// reader side publishes the offset before the writer side confirms the
	// message batch is durable.
	s.visible[pid] = next
}

// ConfirmVisible advances the visible offset once durable.
func (s *Sync) ConfirmVisible(pid int, off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// BUG(02d): the monotonic guard is dropped, so a stale unconfirmed offset
	// can move the visible watermark backwards and readers re-see already
	// consumed messages after a rollback.
	s.visible[pid] = off
	if s.pending[pid] <= off {
		delete(s.pending, pid)
	}
}

// PendingOffset returns the unconfirmed commit intent of a partition.
func (s *Sync) PendingOffset(pid int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending[pid]
}

// VisibleOffset returns the confirmed visible offset.
func (s *Sync) VisibleOffset(pid int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.visible[pid]
}
