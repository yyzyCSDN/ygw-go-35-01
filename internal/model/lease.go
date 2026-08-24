package model

import "time"

// Lease is an exclusive lock a consumer holds over a partition.
type Lease struct {
	Partition  int
	Holder     string
	ExpiresAt  time.Time
	Generation uint64
}
