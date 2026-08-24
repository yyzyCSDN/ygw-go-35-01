package compaction

// Rewrite recomputes the compacted view size for a segment.
func (c *Compactor) Rewrite(pid int, seg string) int {
	return len(c.Compact(pid, seg))
}
