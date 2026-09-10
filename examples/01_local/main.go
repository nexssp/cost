package main

import (
	"context"
	"fmt"

	"github.com/nexssp/cost"
	kernelcost "github.com/nexssp/cost/adapters/kernel"
	"github.com/nexssp/kernel/action"
)

type result struct{ Cost int64 }

func (r result) CostMicros() int64 { return r.Cost }

func main() {
	ledger := cost.NewLedger(int64(cost.ToMicro(1)), cost.USD)
	act := action.New("demo.lookup", func(context.Context, string) (result, error) {
		return result{Cost: int64(cost.ToMicro(0.002))}, nil
	}).
		AnyHook(kernelcost.GuardAction(ledger, int64(cost.ToMicro(0.01)))).
		Build()

	if _, err := act.Do(context.Background(), "nexss"); err != nil {
		panic(err)
	}

	fmt.Printf("used=%d micros spent=%d micros audit_entries=%d\n", ledger.UsedMicros(), ledger.SpentMicros(), len(ledger.Entries()))
}
