package redelivery

import (
	"testing"
	"time"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

func TestRedeliveryPreservesOrder(t *testing.T) {
	s := partition.New()
	tr := New(s, time.Hour)
	m := model.NewMessage("m1", "k", "v", 1)
	tr.Track(1, m)
	due := tr.Due(1, time.Now())
	if len(due) != 0 {
		t.Fatalf("redelivered before deadline: %d", len(due))
	}
}
