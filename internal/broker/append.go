package broker

import (
	"fmt"

	"eventbus/internal/model"
)

// Append publishes a message to a topic partition and returns its offset.
func (b *Broker) Append(topic string, partitionID int, msg *model.Message) (int64, error) {
	t, ok := b.Topic(topic)
	if !ok {
		return 0, fmt.Errorf("broker: topic %s missing", topic)
	}
	p := b.Partition(partitionID)
	if p == nil || p.State() != model.PartitionActive {
		return 0, fmt.Errorf("broker: partition %d not active", partitionID)
	}
	seg := currentSegment(t, partitionID)
	return b.partStore.Append(p, seg, msg), nil
}

func currentSegment(t *model.Topic, pid int) string {
	return "seg-" + itoa(pid)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
