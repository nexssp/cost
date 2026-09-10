package cost

import "time"

// Event is an immutable accounting/audit record. ID should be stable across retries.
type Event struct {
	ID         string    `json:"id,omitempty"`
	Domain     string    `json:"domain"`
	Operation  string    `json:"operation"`
	CostMicros int64     `json:"cost_micros"`
	Currency   Currency  `json:"currency"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
}

// CostReporter lets an action result report actual usage.
type CostReporter interface {
	CostMicros() int64
}
