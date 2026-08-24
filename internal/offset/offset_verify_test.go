package offset

import "testing"

func TestOffsetCommitAfterDurableAppend(t *testing.T) {
	m := New()
	m.Commit(1, 5)
	rec := m.Recover()
	if rec[1] != 0 {
		t.Fatalf("commit advanced before durable: %d", rec[1])
	}
}
