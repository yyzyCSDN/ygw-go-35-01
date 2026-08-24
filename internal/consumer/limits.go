package consumer

// Limits configure consumer behaviour.
type Limits struct {
	MaxFetch int
	IdleTimeoutMs int64
}

// DefaultLimits returns standard consumer limits.
func DefaultLimits() Limits {
	return Limits{MaxFetch: 100, IdleTimeoutMs: 30000}
}

// Bounded reports whether a limit bounds the fetch size.
func (l Limits) Bounded() bool {
	return l.MaxFetch > 0
}
