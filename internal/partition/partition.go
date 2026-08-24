// Package partition stores per-partition message logs in segments.
package partition

import (
	"sync"

	"eventbus/internal/model"
)

// Store keeps partition logs and message segments.
type Store struct {
	mu         sync.RWMutex
	parts      map[int]*model.Partition
	segments   map[int]map[string][]*model.Message
	segmentIdx map[int]map[string]int64
}

// New creates an empty partition store.
func New() *Store {
	return &Store{
		parts:      make(map[int]*model.Partition),
		segments:   make(map[int]map[string][]*model.Message),
		segmentIdx: make(map[int]map[string]int64),
	}
}

// Add registers a partition.
func (s *Store) Add(p *model.Partition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts[p.ID] = p
	s.segments[p.ID] = make(map[string][]*model.Message)
	s.segmentIdx[p.ID] = make(map[string]int64)
}

// Get returns a partition by id.
func (s *Store) Get(id int) *model.Partition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.parts[id]
}

// Append adds a message to a segment and advances the partition offset.
func (s *Store) Append(p *model.Partition, seg string, msg *model.Message) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.Partition = p.ID
	msg.Offset = p.NextOffset
	p.NextOffset++
	s.segments[p.ID][seg] = append(s.segments[p.ID][seg], msg)
	s.segmentIdx[p.ID][seg] = p.NextOffset
	return msg.Offset
}

// Read returns messages from a segment at or after the given offset.
func (s *Store) Read(pid int, seg string, from int64) []*model.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Message, 0)
	for _, m := range s.segments[pid][seg] {
		if m.Offset >= from {
			out = append(out, m)
		}
	}
	return out
}

// SegmentEnd returns the next offset after the last message in a segment.
func (s *Store) SegmentEnd(pid int, seg string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.segmentIdx[pid][seg]
}

// Segments returns the segment names of a partition.
func (s *Store) Segments(pid int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.segments[pid]))
	for seg := range s.segments[pid] {
		out = append(out, seg)
	}
	return out
}

// DropSegment removes a segment (used by retention and compaction).
func (s *Store) DropSegment(pid int, seg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.segments[pid], seg)
	delete(s.segmentIdx[pid], seg)
}
