package consumer

import (
	"eventbus/internal/model"
)

// Handler processes one message and decides ack/dead-letter.
type Handler func(msg *model.Message) error

// Run consumes messages and applies the handler, acking successes.
func (c *Consumer) Run(pid int, seg string, h Handler, max int) int {
	processed := 0
	for _, m := range c.Pull(pid, seg) {
		if max > 0 && processed >= max {
			break
		}
		if h(m) == nil {
			c.Commit(pid, m.Offset+1)
			processed++
		}
	}
	return processed
}
