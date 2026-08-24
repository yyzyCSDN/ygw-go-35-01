package partition

import (
	"sort"
	"sync"
)

// Index is an in-memory posting index from term to message offsets.
type Index struct {
	mu      sync.RWMutex
	posting map[string][]int64
}

// NewIndex creates an empty posting index.
func NewIndex() *Index {
	return &Index{posting: make(map[string][]int64)}
}

// Add records a message offset under a term.
func (ix *Index) Add(term string, offset int64) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.posting[term] = append(ix.posting[term], offset)
}

// Lookup returns sorted offsets for a term.
func (ix *Index) Lookup(term string) []int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := append([]int64(nil), ix.posting[term]...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Terms returns all indexed terms.
func (ix *Index) Terms() []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]string, 0, len(ix.posting))
	for t := range ix.posting {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
