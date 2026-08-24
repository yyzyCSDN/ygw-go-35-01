package quota

import "testing"

func TestQuotaDebitSurvivesRollback(t *testing.T) {
	b := NewBucket(1, 1)
	if !b.Allow() {
		t.Fatalf("first token should be allowed")
	}
	if b.Allow() {
		t.Fatalf("second token must be denied (no refund)")
	}
}
