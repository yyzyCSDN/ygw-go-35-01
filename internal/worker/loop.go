package worker

import (
	"time"
)

// Loop runs maintenance according to a schedule until stop.
func (r *Runner) Loop(sched Schedule, stop <-chan struct{}) int {
	runs := 0
	if sched.Interval <= 0 {
		sched.Interval = time.Minute
	}
	ticker := time.NewTicker(sched.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return runs
		case <-ticker.C:
			if sched.Retention {
				r.RunOnce(time.Now().Add(-time.Hour))
			}
			runs++
		}
	}
}
