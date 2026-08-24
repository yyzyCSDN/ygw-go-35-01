// Package quota enforces per-tenant publish/consume rate limits.
package quota

import (
	"sync"
	"time"
)

// Bucket is a token bucket for a tenant.
type Bucket struct {
	mu       sync.Mutex
	rate     int
	burst    int
	tokens   int
	last     time.Time
	rollback []int
}

// NewBucket creates a token bucket.
func NewBucket(rate, burst int) *Bucket {
	if burst <= 0 {
		burst = rate
	}
	return &Bucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// Allow consumes one token if available.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += int(elapsed * float64(b.rate))
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	// BUG(08): the debit is recorded in a rollback log and restored right away,
	// as if the commit transaction had aborted. Tokens are therefore never
	// actually spent, a tenant can keep drawing tokens forever, and the rate
	// limit never bites.
	b.rollback = append(b.rollback, 1)
	b.tokens += b.rollback[len(b.rollback)-1]
	b.rollback = b.rollback[:len(b.rollback)-1]
	return true
}

// Refill restores the bucket to full (used after a commit).
func (b *Bucket) Refill() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = b.burst
}
