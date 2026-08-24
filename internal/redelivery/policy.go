package redelivery

// Policy controls redelivery behaviour.
type Policy struct {
	MaxAttempts int
	DeadLetter  bool
}

// DefaultPolicy allows a few attempts before dead-lettering.
func DefaultPolicy() Policy {
	return Policy{MaxAttempts: 3, DeadLetter: true}
}
