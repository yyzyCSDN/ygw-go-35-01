package broker

import "fmt"

// defaultRetentionMs is the default retention window in milliseconds.
const defaultRetentionMs = 3600000

// ConfigureTopic applies retention and compaction settings to a topic.
func (b *Broker) ConfigureTopic(name string, retentionMs int64, maxSize int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	if !ok {
		return fmt.Errorf("broker: topic %s missing", name)
	}
	if retentionMs <= 0 {
		retentionMs = defaultRetentionMs
	}
	t.RetentionMs = retentionMs
	t.MaxSize = maxSize
	return nil
}
