// Package consumer pulls messages from assigned partitions and manages leases.
package consumer

import (
	"sync"
	"time"

	"eventbus/internal/group"
	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

// Consumer reads messages for a group member.
type Consumer struct {
	mu       sync.Mutex
	group    *group.Coordinator
	offsets  *offset.Manager
	store    *partition.Store
	leases   map[int]model.Lease
	groupID  string
	memberID string
}

// New creates a consumer for a group member.
func New(g *group.Coordinator, o *offset.Manager, s *partition.Store, groupID, memberID string) *Consumer {
	return &Consumer{group: g, offsets: o, store: s, leases: make(map[int]model.Lease), groupID: groupID, memberID: memberID}
}

// AcquireLease takes a lease over a partition.
func (c *Consumer) AcquireLease(pid int, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if l, ok := c.leases[pid]; ok && now.Before(l.ExpiresAt) && l.Holder != c.memberID {
		return false
	}
	c.leases[pid] = model.Lease{Partition: pid, Holder: c.memberID, ExpiresAt: now.Add(ttl)}
	return true
}

// RenewLease extends a lease held by this member.
func (c *Consumer) RenewLease(pid int, ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.leases[pid]
	if !ok || l.Holder != c.memberID {
		return false
	}
	l.ExpiresAt = time.Now().Add(ttl)
	c.leases[pid] = l
	return true
}

// Pull fetches messages from a partition segment starting at the committed offset.
func (c *Consumer) Pull(pid int, seg string) []*model.Message {
	from := c.offsets.Committed(pid)
	return c.store.Read(pid, seg, from)
}

// Commit advances the committed offset for a partition.
func (c *Consumer) Commit(pid int, next int64) {
	c.offsets.Commit(pid, next)
}
