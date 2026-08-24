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
	if p == nil {
		return 0, fmt.Errorf("broker: partition %d not active", partitionID)
	}
	// BUG(03d): the partition lifecycle check is dropped, so an append that
	// raced with a cancel or seal still writes its partial batch into the
	// partition. The visibility layer then reports the whole partition as
	// published and the half-written message is served to consumers.
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
