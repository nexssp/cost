package nexssflow

import (
	"context"

	"github.com/nexssp/cost"
	"github.com/nexssp/kernel/action"
)

// ReportBucket aggregates the audit ring by (Domain, Operation).
type ReportBucket struct {
	Domain      string `json:"domain"`
	Operation   string `json:"operation"`
	Count       int    `json:"count"`
	SpentMicros int64  `json:"spent_micros"`
}

// Report is the shape returned by cost.report.
type Report struct {
	LimitMicros int64          `json:"limit_micros"`
	UsedMicros  int64          `json:"used_micros"`
	SpentMicros int64          `json:"spent_micros"`
	Currency    string         `json:"currency"`
	Buckets     []ReportBucket `json:"buckets"`
}

func reportAction(ledger *cost.Ledger) action.AnyAction {
	return action.New(ID+".report", func(_ context.Context, _ any) (Report, error) {
		entries := ledger.Entries()

		type bucketKey struct{ domain, operation string }
		byKey := make(map[bucketKey]*ReportBucket, len(entries))
		order := make([]bucketKey, 0, len(entries))

		for _, e := range entries {
			k := bucketKey{e.Domain, e.Operation}
			bucket, exists := byKey[k]
			if !exists {
				bucket = &ReportBucket{Domain: e.Domain, Operation: e.Operation}
				byKey[k] = bucket
				order = append(order, k)
			}
			bucket.Count++
			bucket.SpentMicros += e.CostMicros
		}

		buckets := make([]ReportBucket, 0, len(order))
		for _, k := range order {
			buckets = append(buckets, *byKey[k])
		}

		return Report{
			LimitMicros: ledger.LimitMicros(),
			UsedMicros:  ledger.UsedMicros(),
			SpentMicros: ledger.SpentMicros(),
			Currency:    ledger.Currency().String(),
			Buckets:     buckets,
		}, nil
	}).Description("Aggregate the audit ring by action domain and operation").Build()
}
