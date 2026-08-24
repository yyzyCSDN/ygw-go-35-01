package offset

// Policy controls offset commit behaviour.
type Policy struct {
	WaitDurable bool
	AutoCommit  bool
}

// DefaultPolicy requires durable confirmation before advancing.
func DefaultPolicy() Policy {
	return Policy{WaitDurable: true, AutoCommit: true}
}
