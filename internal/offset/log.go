package offset

import (
	"sort"
	"sync"

	"eventbus/internal/model"
)

// Log records every durable commit for audit and recovery.
type Log struct {
	mu   sync.Mutex
	rows []model.Checkpoint
}

// NewLog creates an offset commit log.
func NewLog() *Log {
	return &Log{}
}

// Append records a durable commit.
func (l *Log) Append(cp model.Checkpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows = append(l.rows, cp)
}

// Rows returns all recorded checkpoints sorted by partition.
func (l *Log) Rows() []model.Checkpoint {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := append([]model.Checkpoint(nil), l.rows...)
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
}
