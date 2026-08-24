// Package model defines the core domain types of the event bus: messages,
// partitions, offsets, consumer groups and leases.
package model

import "time"

// Message is a single event stored in a partition.
type Message struct {
	ID        string
	Key       string
	Value     string
	Partition int
	Offset    int64
	Timestamp time.Time
}

// NewMessage builds a message with a timestamp.
func NewMessage(id, key, value string, partition int) *Message {
	return &Message{ID: id, Key: key, Value: value, Partition: partition, Timestamp: time.Now()}
}
