// Package broker owns topics and partitions and routes appends.
package broker

import (
	"fmt"
	"sync"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

// Broker manages topics and their partitions.
type Broker struct {
	mu        sync.RWMutex
	topics    map[string]*model.Topic
	partStore *partition.Store
	nextPart  int
}

// New creates a broker.
func New(ps *partition.Store) *Broker {
	return &Broker{topics: make(map[string]*model.Topic), partStore: ps, nextPart: 1}
}

// CreateTopic registers a topic with the given partition count.
func (b *Broker) CreateTopic(name string, partitions int, compact bool) (*model.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return nil, fmt.Errorf("broker: topic %s exists", name)
	}
	t := &model.Topic{Name: name, Compact: compact}
	for i := 0; i < partitions; i++ {
		p := model.NewPartition(name, b.nextPart)
		b.nextPart++
		t.Partitions = append(t.Partitions, p.ID)
		b.partStore.Add(p)
	}
	b.topics[name] = t
	return t, nil
}

// Topic returns a topic by name.
func (b *Broker) Topic(name string) (*model.Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

// DeleteTopic removes a topic and retires its partitions.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	if !ok {
		return fmt.Errorf("broker: topic %s missing", name)
	}
	for _, pid := range t.Partitions {
		if p := b.partStore.Get(pid); p != nil {
			p.SetState(model.PartitionRetired)
		}
	}
	delete(b.topics, name)
	return nil
}

// Topics returns all topic names.
func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.topics))
	for name := range b.topics {
		out = append(out, name)
	}
	return out
}

// Partition returns a partition by id.
func (b *Broker) Partition(id int) *model.Partition {
	return b.partStore.Get(id)
}

// Segments returns segment names of a partition.
func (b *Broker) Segments(id int) []string {
	return b.partStore.Segments(id)
}
