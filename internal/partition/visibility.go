package partition

import "sync"

// Visibility tracks which segments of a partition are published and readable.
type Visibility struct {
	mu      sync.RWMutex
	visible map[int]map[string]bool
}

// NewVisibility creates a visibility tracker.
func NewVisibility() *Visibility {
	return &Visibility{visible: make(map[int]map[string]bool)}
}

// Publish marks a single segment visible. Only an explicitly published segment
// becomes readable, so a partial segment left behind by a cancelled append
// stays out of the visible set until the whole batch is published.
func (v *Visibility) Publish(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.visible[pid] == nil {
		v.visible[pid] = make(map[string]bool)
	}
	v.visible[pid][seg] = true
}

// Visible reports whether a segment is readable. The check is segment-exact:
// an unpublished segment — for example the half-written remnant of a batch
// whose append was cancelled — is never reported readable, even after another
// segment of the same partition has been published.
func (v *Visibility) Visible(pid int, seg string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.visible[pid][seg]
}

// Retire removes a segment from the visible set. Because visibility is tracked
// per segment, a retired segment stays unreadable afterwards.
func (v *Visibility) Retire(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.visible[pid] != nil {
		delete(v.visible[pid], seg)
	}
}
