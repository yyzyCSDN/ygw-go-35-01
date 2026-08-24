// Package group coordinates consumer groups: membership, heartbeats and
// partition assignment.
package group

import (
	"sort"
	"sync"
	"time"

	"eventbus/internal/model"
)

// Coordinator manages consumer groups.
type Coordinator struct {
	mu     sync.Mutex
	groups map[string]*model.Group
	now    func() time.Time
}

// New creates a coordinator.
func New() *Coordinator {
	return &Coordinator{groups: make(map[string]*model.Group), now: time.Now}
}

// Join adds a member to a group and bumps the group version.
func (c *Coordinator) Join(group, member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		g = model.NewGroup(group)
		c.groups[group] = g
	}
	if _, ok := g.Members[member]; !ok {
		g.Members[member] = &model.Member{ID: member}
		g.Version++
	}
	g.Members[member].LastHeartbeat = c.now()
}

// Heartbeat refreshes a member's heartbeat.
func (c *Coordinator) Heartbeat(group, member string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return false
	}
	m := g.Members[member]
	if m == nil {
		return false
	}
	m.LastHeartbeat = c.now()
	return true
}

// Ack records a processed message time for a member.
func (c *Coordinator) Ack(group, member string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return false
	}
	m := g.Members[member]
	if m == nil {
		return false
	}
	m.LastAck = c.now()
	return true
}

// Members returns member ids of a group.
func (c *Coordinator) Members(group string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return nil
	}
	out := make([]string, 0, len(g.Members))
	for id := range g.Members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Remove drops a member from a group and bumps the version.
func (c *Coordinator) Remove(group, member string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[group]
	if g == nil {
		return
	}
	if _, ok := g.Members[member]; ok {
		delete(g.Members, member)
		g.Version++
	}
}
