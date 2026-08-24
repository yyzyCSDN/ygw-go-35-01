package model

// Topic is a named log divided into partitions.
type Topic struct {
	Name       string
	Partitions []int
	Compact    bool
	RetentionMs int64
	MaxSize    int
}
