package model

import "time"

// Member is a consumer that belongs to a group and holds partitions.
type Member struct {
	ID            string
	Partitions    []int
	LastHeartbeat time.Time
	LastAck       time.Time
}

// Group is a set of consumers sharing topic partitions.
type Group struct {
	Name    string
	Members map[string]*Member
	Version uint64
}

// NewGroup creates a group with no members.
func NewGroup(name string) *Group {
	return &Group{Name: name, Members: make(map[string]*Member)}
}
