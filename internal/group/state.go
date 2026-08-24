package group

import "eventbus/internal/model"

// GroupState is a snapshot of a group's membership and assignment.
type GroupState struct {
	Name    string
	Version uint64
	Count   int
}

// State returns a compact group state.
func (c *Coordinator) State(name string) GroupState {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[name]
	if g == nil {
		return GroupState{Name: name}
	}
	return GroupState{Name: name, Version: g.Version, Count: len(g.Members)}
}

// Reset clears a group's members.
func (c *Coordinator) Reset(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if g := c.groups[name]; g != nil {
		g.Members = make(map[string]*model.Member)
		g.Version++
	}
}
