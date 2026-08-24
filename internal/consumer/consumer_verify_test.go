package consumer

import (
	"testing"
	"time"

	"eventbus/internal/group"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

func TestExpiredLeaseEvictsMember(t *testing.T) {
	g := group.New()
	o := offset.New()
	s := partition.New()
	c1 := New(g, o, s, "g", "c1")
	c2 := New(g, o, s, "g", "c2")
	c1.AcquireLease(1, time.Minute)
	if c2.RenewLease(1, time.Minute) {
		t.Fatalf("non-holder renewed the lease")
	}
}
