package partition

import (
	"sync"

	"eventbus/internal/model"
)

// Writer buffers appends for a segment and flushes them to the store.
type Writer struct {
	mu      sync.Mutex
	store   *Store
	seg     string
	pending []*model.Message
	closed  bool
}

// NewWriter creates a segment writer.
func NewWriter(s *Store, seg string) *Writer {
	return &Writer{store: s, seg: seg}
}

// Write buffers one message.
func (w *Writer) Write(p *model.Partition, msg *model.Message) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return -1
	}
	w.pending = append(w.pending, msg)
	return p.NextOffset
}

// Flush writes all buffered messages to the store atomically.
func (w *Writer) Flush(p *model.Partition) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, m := range w.pending {
		w.store.Append(p, w.seg, m)
		n++
	}
	w.pending = w.pending[:0]
	return n
}

// Close flushes and marks the writer closed.
func (w *Writer) Close(p *model.Partition) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0
	}
	w.closed = true
	n := 0
	for _, m := range w.pending {
		w.store.Append(p, w.seg, m)
		n++
	}
	w.pending = nil
	return n
}

// Pending returns the number of buffered messages.
func (w *Writer) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}
