package partition

import "testing"

func TestCancelledAppendNotVisible(t *testing.T) {
	v := NewVisibility()
	v.Publish(1, "seg-1")
	if v.Visible(1, "seg-2") {
		t.Fatalf("unpublished segment became visible")
	}
}
