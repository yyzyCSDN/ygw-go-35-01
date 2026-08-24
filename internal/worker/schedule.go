package worker

import "time"

// Schedule describes one maintenance run.
type Schedule struct {
	Interval time.Duration
	Retention bool
	Compaction bool
}

// DefaultSchedule is the standard maintenance cadence.
func DefaultSchedule() Schedule {
	return Schedule{Interval: time.Minute, Retention: true, Compaction: true}
}
