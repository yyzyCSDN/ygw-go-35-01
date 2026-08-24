// Package metric accumulates broker-level counters.
package metric

import "sync"

// Collector tracks broker counters.
type Collector struct {
	mu       sync.Mutex
	appends  int
	reads    int
	rebals   int
	errors   int
}

// New creates a collector.
func New() *Collector {
	return &Collector{}
}

// RecordAppend increments the append counter.
func (c *Collector) RecordAppend() {
	c.mu.Lock()
	c.appends++
	c.mu.Unlock()
}

// RecordRead increments the read counter.
func (c *Collector) RecordRead() {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
}

// RecordRebalance increments the rebalance counter.
func (c *Collector) RecordRebalance() {
	c.mu.Lock()
	c.rebals++
	c.mu.Unlock()
}

// RecordError increments the error counter.
func (c *Collector) RecordError() {
	c.mu.Lock()
	c.errors++
	c.mu.Unlock()
}

// Snapshot returns current counters.
func (c *Collector) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]int{"appends": c.appends, "reads": c.reads, "rebalances": c.rebals, "errors": c.errors}
}
