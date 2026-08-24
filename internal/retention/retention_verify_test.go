package retention

import (
	"testing"
	"time"

	"eventbus/internal/model"
	"eventbus/internal/offset"
	"eventbus/internal/partition"
)

func TestRetentionKeepsUnconsumedSegment(t *testing.T) {
	s := partition.New()
	p := model.NewPartition("t", 1)
	s.Add(p)
	old := time.Now().Add(-2 * time.Hour)
	s.Append(p, "seg-1", &model.Message{ID: "m1", Key: "k", Value: "v", Timestamp: old})
	om := offset.New()
	cl := New(s, om)
	n := cl.Clean(p, "seg-1", time.Now().Add(-time.Hour))
	if n != 0 {
		t.Fatalf("dropped unconsumed segment: %d", n)
	}
	if len(s.Segments(1)) == 0 {
		t.Fatalf("segment removed while unconsumed")
	}
}
