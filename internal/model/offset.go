package model

// OffsetState tracks the committed consumer offset and the next message offset.
type OffsetState struct {
	Partition int
	Committed int64
	Next      int64
}
