package model

// EventRecord is a durable event emitted by the broker for audit.
type EventRecord struct {
	Type   string
	Topic  string
	Partition int
	Offset int64
	Key    string
}

// NewEventRecord builds an audit record.
func NewEventRecord(typ, topic string, pid int, off int64, key string) EventRecord {
	return EventRecord{Type: typ, Topic: topic, Partition: pid, Offset: off, Key: key}
}
