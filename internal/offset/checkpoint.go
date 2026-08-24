package offset

import (
	"sort"

	"eventbus/internal/model"
)

// Checkpoints returns durable offset checkpoints for all partitions.
func (m *Manager) Checkpoints() []model.Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.Checkpoint, 0, len(m.durable))
	for pid, off := range m.durable {
		out = append(out, model.Checkpoint{Partition: pid, Offset: off, Segment: "seg-" + itoa(pid)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
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
