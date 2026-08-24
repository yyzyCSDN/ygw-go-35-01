package consumer

import (
	"eventbus/internal/model"
)

// PullBounded fetches up to limit messages from a segment.
func (c *Consumer) PullBounded(pid int, seg string, limit int) []*model.Message {
	all := c.Pull(pid, seg)
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}
