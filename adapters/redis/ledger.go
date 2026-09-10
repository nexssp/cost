package redis

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nexssp/cost"
	redisclient "github.com/redis/go-redis/v9"
)

var reserveScript = redisclient.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')

-- Use Redis server clock as single source of truth
local time_res = redis.call('TIME')
local now = tonumber(time_res[1])

-- Bounded lazy reclamation: limit to 100 entries to protect Redis single thread
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 100)
for _, id in ipairs(expired) do
    local key = KEYS[2] .. ':' .. id
    local value = tonumber(redis.call('GET', key) or '0')
    if value > 0 then
        used = used - value
    end
    redis.call('DEL', key)
    redis.call('ZREM', KEYS[3], id)
end

local limit = tonumber(ARGV[1])
local est = tonumber(ARGV[2])
local id = ARGV[3]
local ttl = tonumber(ARGV[4])

if limit >= 0 and used + est > limit then
    redis.call('SET', KEYS[1], used)
    return redis.error_reply('BUDGET_EXCEEDED')
end

used = used + est
redis.call('SET', KEYS[1], used)

-- Safety margin (ttl + 86400) guarantees the key persists until lazy reclamation
redis.call('SET', KEYS[2] .. ':' .. id, est, 'EX', ttl + 86400)
redis.call('ZADD', KEYS[3], now + ttl, id)

return used
`)

var terminalScript = redisclient.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')

local time_res = redis.call('TIME')
local now = tonumber(time_res[1])

local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 100)
for _, exp_id in ipairs(expired) do
    local exp_key = KEYS[2] .. ':' .. exp_id
    local exp_val = tonumber(redis.call('GET', exp_key) or '0')
    if exp_val > 0 then
        used = used - exp_val
    end
    redis.call('DEL', exp_key)
    redis.call('ZREM', KEYS[3], exp_id)
end

local id = ARGV[1]
local actual = tonumber(ARGV[2])
local key = KEYS[2] .. ':' .. id
local reserved = tonumber(redis.call('GET', key) or '0')
if reserved == 0 then
    redis.call('SET', KEYS[1], used)
    return redis.error_reply('RESERVATION_NOT_FOUND')
end

local new_used = used + actual - reserved
redis.call('SET', KEYS[1], new_used)
redis.call('DEL', key)
redis.call('ZREM', KEYS[3], id)

return new_used
`)

var releaseScript = redisclient.NewScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')

local time_res = redis.call('TIME')
local now = tonumber(time_res[1])

local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 100)
for _, exp_id in ipairs(expired) do
    local exp_key = KEYS[2] .. ':' .. exp_id
    local exp_val = tonumber(redis.call('GET', exp_key) or '0')
    if exp_val > 0 then
        used = used - exp_val
    end
    redis.call('DEL', exp_key)
    redis.call('ZREM', KEYS[3], exp_id)
end

local id = ARGV[1]
local key = KEYS[2] .. ':' .. id
local reserved = tonumber(redis.call('GET', key) or '0')
if reserved == 0 then
    redis.call('SET', KEYS[1], used)
    return redis.error_reply('RESERVATION_NOT_FOUND')
end

local new_used = used - reserved
redis.call('SET', KEYS[1], new_used)
redis.call('DEL', key)
redis.call('ZREM', KEYS[3], id)

return new_used
`)

type Ledger struct {
	client   redisclient.UniversalClient
	prefix   string
	limit    int64
	currency cost.Currency
	ttl      time.Duration

	usedKey        string
	reservationKey string
	expiryKey      string
	keys           []string
}

type Config struct {
	Client         redisclient.UniversalClient
	Prefix         string
	LimitMicros    int64
	Currency       cost.Currency
	ReservationTTL time.Duration
}

func New(cfg Config) *Ledger {
	ttl := cfg.ReservationTTL
	if ttl < time.Second {
		ttl = 30 * time.Second
	}

	usedKey := cfg.Prefix + ":used"
	reservationKey := cfg.Prefix + ":resv"
	expiryKey := cfg.Prefix + ":expiry"

	return &Ledger{
		client:         cfg.Client,
		prefix:         cfg.Prefix,
		limit:          cfg.LimitMicros,
		currency:       cfg.Currency,
		ttl:            ttl,
		usedKey:        usedKey,
		reservationKey: reservationKey,
		expiryKey:      expiryKey,
		keys:           []string{usedKey, reservationKey, expiryKey},
	}
}

func (l *Ledger) Reserve(ctx context.Context, estimate int64) (cost.Reservation, error) {
	if ctx == nil {
		return nil, cost.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if l.client == nil {
		return nil, fmt.Errorf("redis: nil client")
	}

	if estimate < 0 || estimate > math.MaxInt64/2 {
		return nil, &cost.ValidationError{
			Field: "estimate_micros",
			Value: estimate,
		}
	}

	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("redis: reservation id: %w", err)
	}

	id := fmt.Sprintf("%d-%x", time.Now().UnixNano(), binary.LittleEndian.Uint64(raw[:]))
	ttlSeconds := int64(l.ttl / time.Second)
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	err := reserveScript.Run(ctx, l.client, l.keys, l.limit, estimate, id, ttlSeconds).Err()
	if err != nil {
		if redisclient.HasErrorPrefix(err, "BUDGET_EXCEEDED") {
			return nil, fmt.Errorf("%w: %v", cost.ErrBudgetExceeded, err)
		}

		return nil, fmt.Errorf("redis: reserve: %w", err)
	}

	return &reservation{
		ledger: l,
		id:     id,
	}, nil
}

func (l *Ledger) Record(ctx context.Context, event cost.Event) error {
	if ctx == nil {
		return cost.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if event.CostMicros <= 0 {
		return &cost.ValidationError{
			Field: "cost_micros",
			Value: event.CostMicros,
		}
	}

	if event.Currency == (cost.Currency{}) {
		event.Currency = l.currency
	}

	if event.Currency != l.currency {
		return fmt.Errorf("redis: currency mismatch")
	}

	key := fmt.Sprintf("%s:spent:%s:%s", l.prefix, event.Domain, event.Operation)

	return l.client.IncrBy(ctx, key, event.CostMicros).Err()
}

type reservation struct {
	ledger *Ledger
	id     string
	mu     sync.Mutex
	done   bool
}

func (r *reservation) Commit(ctx context.Context, actual int64) error {
	if ctx == nil {
		return cost.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.done {
		return nil
	}

	if actual < 0 {
		actual = 0
	}

	if err := terminalScript.Run(ctx, r.ledger.client, r.ledger.keys, r.id, actual).Err(); err != nil {
		if redisclient.HasErrorPrefix(err, "RESERVATION_NOT_FOUND") {
			return fmt.Errorf("%w: %v", cost.ErrReservationNotFound, err)
		}

		return fmt.Errorf("redis: commit: %w", err)
	}

	r.done = true

	return nil
}

func (r *reservation) Release(ctx context.Context) error {
	if ctx == nil {
		return cost.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.done {
		return nil
	}

	if err := releaseScript.Run(ctx, r.ledger.client, r.ledger.keys, r.id).Err(); err != nil {
		if redisclient.HasErrorPrefix(err, "RESERVATION_NOT_FOUND") {
			return fmt.Errorf("%w: %v", cost.ErrReservationNotFound, err)
		}

		return fmt.Errorf("redis: release: %w", err)
	}

	r.done = true

	return nil
}
