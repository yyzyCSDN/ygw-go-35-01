package model

// Stats is a compact report of a partition's state.
type Stats struct {
	Partition int
	State     string
	NextOffset int64
	Segments  int
}
