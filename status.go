package outboxd

import (
	"sync"
	"time"
)

// Status is a point-in-time snapshot of a relay's delivery progress. It is
// the signal that makes a stalled relay visible: a poison message that keeps
// failing shows up as Retrying=true with a growing RetryingSince age, while
// Delivered/LastDeliveredAt stop moving. Export it to your metrics system
// and alert on it — with MaxRetries=0 (the default) a deterministically
// failing message blocks delivery forever and pins the replication slot.
type Status struct {
	// Delivered is the number of messages successfully handled since the
	// relay was created. It survives reconnects.
	Delivered uint64 `json:"delivered"`
	// LastDeliveredAt is the time of the most recent successful delivery.
	LastDeliveredAt time.Time `json:"last_delivered_at,omitzero"`
	// Retrying reports whether delivery is currently blocked on a message
	// whose handler keeps failing.
	Retrying bool `json:"retrying"`
	// RetryingID is the ID of the blocking message.
	RetryingID int64 `json:"retrying_id,omitzero"`
	// RetryAttempts is the number of failed attempts recorded so far for
	// the blocking message.
	RetryAttempts int `json:"retry_attempts,omitzero"`
	// RetryingSince is the time of the blocking message's first failure.
	RetryingSince time.Time `json:"retrying_since,omitzero"`
}

type progressTracker struct {
	mu   sync.Mutex
	stat Status
}

func (p *progressTracker) delivered(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stat.Delivered++
	p.stat.LastDeliveredAt = now
	p.clearRetryLocked()
}

func (p *progressTracker) retrying(id int64, attempts int, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stat.Retrying || p.stat.RetryingID != id {
		p.stat.RetryingSince = now
	}
	p.stat.Retrying = true
	p.stat.RetryingID = id
	p.stat.RetryAttempts = attempts
}

// resolved clears the retry state when the blocking message is settled
// without a successful delivery (dropped after MaxRetries).
func (p *progressTracker) resolved() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearRetryLocked()
}

func (p *progressTracker) clearRetryLocked() {
	p.stat.Retrying = false
	p.stat.RetryingID = 0
	p.stat.RetryAttempts = 0
	p.stat.RetryingSince = time.Time{}
}

func (p *progressTracker) snapshot() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stat
}

// Status returns a snapshot of the relay's delivery progress. It is safe to
// call from any goroutine while the relay is running.
func (r *Relay) Status() Status {
	return r.progress.snapshot()
}
