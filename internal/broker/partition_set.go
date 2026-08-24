package broker

import (
	"sort"
	"sync"

	"eventbus/internal/model"
)

// PartitionSet is an ordered registry of partitions for a topic.
type PartitionSet struct {
	mu         sync.Mutex
	topic      string
	partitions []int
}

// NewPartitionSet creates a set for a topic.
func NewPartitionSet(topic string) *PartitionSet {
	return &PartitionSet{topic: topic}
}

// Add registers a partition.
func (s *PartitionSet) Add(pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.partitions {
		if p == pid {
			return
		}
	}
	s.partitions = append(s.partitions, pid)
	sort.Ints(s.partitions)
}

// Remove drops a partition.
func (s *PartitionSet) Remove(pid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.partitions[:0]
	for _, p := range s.partitions {
		if p != pid {
			out = append(out, p)
		}
	}
	s.partitions = out
}

// List returns sorted partition ids.
func (s *PartitionSet) List() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.partitions...)
}

// Contains reports whether a partition is registered.
func (s *PartitionSet) Contains(pid int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.partitions {
		if p == pid {
			return true
		}
	}
	return false
}

// Snapshot returns the set as model partition references.
func (s *PartitionSet) Snapshot(get func(int) *model.Partition) []*model.Partition {
	out := make([]*model.Partition, 0)
	for _, pid := range s.List() {
		if p := get(pid); p != nil {
			out = append(out, p)
		}
	}
	return out
}
