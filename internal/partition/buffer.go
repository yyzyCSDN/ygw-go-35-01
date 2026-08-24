package partition

import (
	"sort"
	"sync"

	"eventbus/internal/model"
)

// Batch is a group of messages written together.
type Batch struct {
	mu     sync.Mutex
	msgs   []*model.Message
	closed bool
}

// NewBatch creates an empty batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Add buffers a message.
func (b *Batch) Add(msg *model.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.msgs = append(b.msgs, msg)
	}
}

// Close seals the batch.
func (b *Batch) Close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

// Size returns the number of buffered messages.
func (b *Batch) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.msgs)
}

// Offsets returns sorted offsets of the batch (assigned at append time).
func (b *Batch) Offsets() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int64, 0, len(b.msgs))
	for _, m := range b.msgs {
		out = append(out, m.Offset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
