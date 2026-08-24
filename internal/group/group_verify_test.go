package group

import (
	"testing"
	"time"
)

func TestHeartbeatUsesHeartbeatTime(t *testing.T) {
	c := New()
	c.Join("g", "a")
	stale := c.HeartbeatTimeout("g", time.Hour)
	if len(stale) != 0 {
		t.Fatalf("active member flagged stale: %v", stale)
	}
}
