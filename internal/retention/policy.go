package retention

// Policy configures retention for a topic.
type Policy struct {
	MaxAge  int64
	MaxSize int
}

// DefaultPolicy returns a one-hour retention policy.
func DefaultPolicy() Policy {
	return Policy{MaxAge: 3600000, MaxSize: 1024 * 1024}
}

// AgeExpired reports whether an age in milliseconds exceeds the policy.
func (p Policy) AgeExpired(ageMs int64) bool {
	return p.MaxAge > 0 && ageMs >= p.MaxAge
}
