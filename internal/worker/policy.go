package worker

// Policy decides which maintenance tasks run in a pass.
type Policy struct {
	Retention      bool
	Compaction     bool
	EvictStale     bool
}

// DefaultPolicy enables all maintenance tasks.
func DefaultMaintenancePolicy() Policy {
	return Policy{Retention: true, Compaction: true, EvictStale: true}
}
