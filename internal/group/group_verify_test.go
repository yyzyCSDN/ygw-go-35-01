package group

import "testing"

func TestRebalanceUsesLatestAssignment(t *testing.T) {
	c := New()
	c.Join("g", "a")
	c.Join("g", "b")
	c.Rebalance("g", []int{1, 2})
	c.Remove("g", "b")
	as := c.Rebalance("g", []int{1, 2})
	if _, ok := as["b"]; ok {
		t.Fatalf("removed member still holds partitions")
	}
}
