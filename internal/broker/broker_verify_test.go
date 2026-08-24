package broker

import (
	"testing"

	"eventbus/internal/model"
	"eventbus/internal/partition"
)

func TestDeleteNotOverwrittenByInflightAppend(t *testing.T) {
	ps := partition.New()
	b := New(ps)
	if _, err := b.CreateTopic("t", 1, false); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteTopic("t"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Append("t", 1, model.NewMessage("m", "k", "v", 1)); err == nil {
		t.Fatalf("append succeeded after delete")
	}
}
