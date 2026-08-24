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

// Publish marks a segment visible.
func (v *Visibility) Publish(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.visible[pid] == nil {
		v.visible[pid] = make(map[string]bool)
	}
	v.visible[pid][seg] = true
}

// Visible reports whether a segment is readable.
func (v *Visibility) Visible(pid int, seg string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.visible[pid][seg]
}

// Retire removes a segment from the visible set.
func (v *Visibility) Retire(pid int, seg string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.visible[pid], seg)
}
