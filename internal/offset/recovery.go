package offset

import "eventbus/internal/model"

// RecoverFromLog restores durable offsets from a commit log.
func (m *Manager) RecoverFromLog(rows []model.Checkpoint) map[int]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range rows {
		m.durable[r.Partition] = r.Offset
		m.commits[r.Partition] = r.Offset
	}
	out := make(map[int]int64, len(m.durable))
	for k, v := range m.durable {
		out[k] = v
	}
	return out
}
