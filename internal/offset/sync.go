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
//
// The intent stays in pending and does not become visible until the writer
// side confirms the batch is durable, so readers never see an offset that
// points past messages that may not yet be on disk.
func (s *Sync) MarkPending(pid int, next int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[pid] = next
}

// ConfirmVisible advances the visible offset once durable.
//
// The visible watermark advances only forward, so a stale or out-of-order
// confirmation cannot regress it and re-expose already consumed messages.
func (s *Sync) ConfirmVisible(pid int, off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off > s.visible[pid] {
		s.visible[pid] = off
	}
	if s.pending[pid] <= s.visible[pid] {
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
