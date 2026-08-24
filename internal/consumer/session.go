package consumer

import "time"

// Session tracks a consumer's lease and activity.
type Session struct {
	MemberID string
	Started  time.Time
	LastIO   time.Time
}

// NewSession creates a consumer session.
func NewSession(memberID string) *Session {
	now := time.Now()
	return &Session{MemberID: memberID, Started: now, LastIO: now}
}

// Touch updates the last activity time.
func (s *Session) Touch() {
	s.LastIO = time.Now()
}

// Idle returns how long the session has been inactive.
func (s *Session) Idle() time.Duration {
	return time.Since(s.LastIO)
}
