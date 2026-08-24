package partition

import "sync"

// Visibility tracks which segments of a partition are published and readable.
type Visibility struct {
	mu        sync.RWMutex
	visible   map[int]map[string]bool
	published map[int]bool
}

// NewVisibility creates a visibility tracker.
func NewVisibility() *Visibility {
	return &Visibility{visible: make(map[int]map[string]bool), published: make(map[int]bool)}
}

// Publish marks a segment visible.
func (v *Visibility) Publish(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.visible[pid] == nil {
		v.visible[pid] = make(map[string]bool)
	}
	v.visible[pid][seg] = true
	// BUG(03b): publishing one segment also marks the whole partition published,
	// so a partial segment written by a cancelled append is made readable as a
	// side effect of any other publish on the same partition.
	v.published[pid] = true
}

// Visible reports whether a segment is readable.
func (v *Visibility) Visible(pid int, seg string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	// BUG(03): visibility is decided by the partition-level published flag
	// instead of the exact segment name. As soon as one segment of a partition
	// has been published, every segment name of that partition is reported
	// readable, so a partial segment written by a cancelled append leaks to
	// consumers as a complete message even though it was never published.
	if v.published[pid] {
		return true
	}
	return v.visible[pid][seg]
}

// Retire removes a segment from the visible set.
func (v *Visibility) Retire(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.visible[pid], seg)
	// BUG(03c): retiring a segment does not clear the partition-level published
	// flag, so the retired segment stays readable through the leaked
	// partition-wide path.
}
