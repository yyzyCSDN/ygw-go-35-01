package model

// Config holds broker-level runtime settings.
type Config struct {
	PartitionCount int
	RetentionMs    int64
	Compaction     bool
}

// DefaultConfig returns standard broker settings.
func DefaultConfig() Config {
	return Config{PartitionCount: 3, RetentionMs: 3600000, Compaction: true}
}
