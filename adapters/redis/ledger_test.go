package redis_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nexssp/cost"
	redisadapter "github.com/nexssp/cost/adapters/redis"
	"github.com/nexssp/cost/testkit"
	redisclient "github.com/redis/go-redis/v9"
)

func TestLedgerContract(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	testkit.RunContract(t, func(t *testing.T) cost.Reserver {
		return redisadapter.New(redisadapter.Config{
			Client:      client,
			Prefix:      "{test}:contract",
			LimitMicros: 100,
			Currency:    cost.USD,
		})
	})
}

func TestLedgerLifecycle(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	ledger := redisadapter.New(redisadapter.Config{
		Client:      client,
		Prefix:      "{test}:budget",
		LimitMicros: 1_000_000,
		Currency:    cost.USD,
	})

	reservation, err := ledger.Reserve(context.Background(), 400_000)
	if err != nil {
		t.Fatal(err)
	}

	if err := reservation.Commit(context.Background(), 250_000); err != nil {
		t.Fatal(err)
	}

	if _, err := ledger.Reserve(context.Background(), 800_001); !errors.Is(err, cost.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}

	event := cost.Event{
		ID:         "e1",
		Domain:     "test",
		Operation:  "call",
		CostMicros: 250_000,
		Currency:   cost.USD,
	}
	if err := ledger.Record(context.Background(), event); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerExpirationReclamation(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	now := time.Now()
	s.SetTime(now)

	ledger := redisadapter.New(redisadapter.Config{
		Client:         client,
		Prefix:         "{test}:reclaim",
		LimitMicros:    1_000_000,
		Currency:       cost.USD,
		ReservationTTL: time.Second,
	})

	// 1. Rezerwujemy 100% budżetu
	_, err := ledger.Reserve(context.Background(), 1_000_000)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Budżet jest pełny - kolejna próba musi zostać odrzucona
	if _, err := ledger.Reserve(context.Background(), 1); err == nil {
		t.Fatal("expected rejection on exhausted budget")
	}

	// 3. Symulujemy upływ czasu w miniredis:
	// SetTime aktualizuje czas zwracany przez komendę TIME w Lua,
	// a FastForward przesuwa TTL kluczy.
	s.SetTime(now.Add(2 * time.Second))
	s.FastForward(2 * time.Second)

	// 4. Następna rezerwacja musi zrekultywować wygasły budżet i przejść pomyślnie
	r2, err := ledger.Reserve(context.Background(), 500_000)
	if err != nil {
		t.Fatalf("expected reclamation, got error: %v", err)
	}

	if err := r2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerLazyReclamationBatching(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	prefix := "{test}:batch"
	ledger := redisadapter.New(redisadapter.Config{
		Client:         client,
		Prefix:         prefix,
		LimitMicros:    1_000_000,
		Currency:       cost.USD,
		ReservationTTL: time.Second,
	})

	// Wstrzykujemy 250 wygasłych rezerwacji z czasem 100 sekund w przeszłości
	now := time.Now().Unix() - 100
	pipe := client.Pipeline()
	for i := 1; i <= 250; i++ {
		id := fmt.Sprintf("exp-%d", i)
		pipe.Set(context.Background(), fmt.Sprintf("%s:resv:%s", prefix, id), 100, 24*time.Hour)
		pipe.ZAdd(context.Background(), prefix+":expiry", redisclient.Z{Score: float64(now), Member: id})
	}
	pipe.Set(context.Background(), prefix+":used", 25000, 0)
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Rezerwacja powinna uruchomić partię czyszczenia ograniczoną do dokładnie 100 wpisów
	_, err := ledger.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	// Weryfikujemy ile WYGASŁYCH wpisów (score <= now) pozostało w sorted secie.
	// Z 250 wygasłych usunięto dokładnie 100, więc musi pozostać dokładnie 150 wygasłych.
	expiredRemaining, err := client.ZCount(context.Background(), prefix+":expiry", "-inf", fmt.Sprint(now)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if expiredRemaining != 150 {
		t.Fatalf("expected exactly 150 expired items remaining after 100-batch reclamation, got %d", expiredRemaining)
	}
}

func TestLedgerConcurrentHighContention(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	ledger := redisadapter.New(redisadapter.Config{
		Client:      client,
		Prefix:      "{test}:race",
		LimitMicros: 20_000,
		Currency:    cost.USD,
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	reservations := make([]cost.Reservation, 0, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := ledger.Reserve(context.Background(), 500)
			if err == nil {
				mu.Lock()
				reservations = append(reservations, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(reservations) != 40 {
		t.Fatalf("expected exactly 40 reservations (20_000 / 500), got %d", len(reservations))
	}

	for _, r := range reservations {
		if err := r.Commit(context.Background(), 500); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLedgerExpiredCommitReturnsNotFound(t *testing.T) {
	s := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: s.Addr()})
	defer client.Close()

	now := time.Now()
	s.SetTime(now)

	ledger := redisadapter.New(redisadapter.Config{
		Client:         client,
		Prefix:         "{test}:notfound",
		LimitMicros:    1_000_000,
		Currency:       cost.USD,
		ReservationTTL: time.Second,
	})

	r, err := ledger.Reserve(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}

	// Przesuwamy czas w Redis o 2 sekundy (rezerwacja wygasa)
	s.SetTime(now.Add(2 * time.Second))
	s.FastForward(2 * time.Second)

	// Inna operacja uruchamia leniwe czyszczenie i usuwa wygasłą rezerwację
	_, _ = ledger.Reserve(context.Background(), 10)

	// Commit na usuniętej, wygasłej rezerwacji MUSI zwrócić ErrReservationNotFound
	if err := r.Commit(context.Background(), 100); !errors.Is(err, cost.ErrReservationNotFound) {
		t.Fatalf("expected ErrReservationNotFound, got %v", err)
	}
}
