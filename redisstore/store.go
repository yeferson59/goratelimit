// Package redisstore implements core.Store over Redis, to deploy the
// rate limiter across multiple instances sharing a single count.
//
// The token bucket's state and logic live in a Lua script that Redis
// executes atomically: reading tokens, refilling based on elapsed time,
// consuming, and setting the TTL all happen in a single operation, with no
// race conditions between instances (a separate GET + SET would let two
// instances read the same balance and both consume).
//
// Redis provides the time (TIME inside the script), not each instance: so
// it doesn't matter if your containers' clocks are out of sync.
//
// This package is optional: if you don't import it, your binary doesn't
// include go-redis. The in-memory store remains the middleware's default.
package redisstore

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yeferson59/goratelimit/core"
)

// allowScript: atomic token bucket.
//
// KEYS[1] = bucket key
// ARGV[1] = rate (tokens per second)
// ARGV[2] = burst (capacity)
// ARGV[3] = cost (tokens to consume; negative = refund)
//
// Returns {allowed, tokens, retry_after_us, reset_after_us} — the floats
// are sent as strings because Redis truncates Lua numbers to integers.
const allowScript = `
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

local data = redis.call('HMGET', key, 'tk', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = tokens + elapsed / 1000000 * rate
if tokens > burst then tokens = burst end

local allowed = 0
local retry_after = 0

if cost < 0 then
  -- refund
  tokens = tokens - cost
  if tokens > burst then tokens = burst end
  allowed = 1
elseif tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  if rate > 0 then
    retry_after = (cost - tokens) / rate * 1000000
  else
    retry_after = -1
  end
end

redis.call('HSET', key, 'tk', tokens, 'ts', now)

-- TTL: once the bucket is full again, the entry is redundant
-- (recreating it gives the same result). 60s margin for rounding.
local ttl = 60
if rate > 0 then
  ttl = math.ceil((burst - tokens) / rate) + 60
end
redis.call('EXPIRE', key, ttl)

local reset_after = 0
if rate > 0 and tokens < burst then
  reset_after = (burst - tokens) / rate * 1000000
end

return {allowed, tostring(tokens), tostring(retry_after), tostring(reset_after)}
`

var script = redis.NewScript(allowScript)

// Options configures the RedisStore.
type Options struct {
	// Prefix is prepended to every key in Redis to avoid colliding with
	// other keys in your application. Default: "rl:".
	Prefix string

	// Timeout per operation against Redis. A rate limiter shouldn't add
	// unbounded latency to every request if Redis degrades.
	// Default: 50ms.
	Timeout time.Duration

	// FailOpen decides what happens if Redis doesn't respond (down, timeout):
	//   true  → allow the request (prioritizes availability; default)
	//   false → reject it (prioritizes protection; for login/payments)
	FailOpen bool

	// OnError is invoked when an operation against Redis fails, so
	// you can log/measure it. May be nil.
	OnError func(err error)
}

// Store implements core.Store over Redis.
type Store struct {
	client redis.Scripter
	opts   Options
}

// Compile-time check: *Store satisfies core.Store.
var _ core.Store = (*Store)(nil)

// New creates a Store over an existing go-redis client. Accepts
// *redis.Client, *redis.ClusterClient, or any redis.Scripter.
func New(client redis.Scripter, opts Options) *Store {
	if opts.Prefix == "" {
		opts.Prefix = "rl:"
	}

	if opts.Timeout <= 0 {
		opts.Timeout = 50 * time.Millisecond
	}

	return new(Store{client: client, opts: opts})
}

func (s *Store) run(key string, cost float64, l core.Limit) (core.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.Timeout)
	defer cancel()

	raw, err := script.Run(ctx, s.client,
		[]string{s.opts.Prefix + key},
		strconv.FormatFloat(l.Rate, 'f', -1, 64),
		strconv.FormatFloat(l.Burst, 'f', -1, 64),
		strconv.FormatFloat(cost, 'f', -1, 64),
	).Slice()
	if err != nil {
		return core.Result{}, err
	}

	allowed, _ := raw[0].(int64)
	tokens := parseFloat(raw[1])
	retryUs := parseFloat(raw[2])
	resetUs := parseFloat(raw[3])

	res := core.Result{
		Allowed:    allowed == 1,
		Remaining:  tokens,
		ResetAfter: time.Duration(resetUs) * time.Microsecond,
	}

	if retryUs < 0 {
		res.RetryAfter = time.Duration(1<<63 - 1) // rate 0: never
	} else {
		res.RetryAfter = time.Duration(retryUs) * time.Microsecond
	}

	return res, nil
}

func parseFloat(v any) float64 {
	s, ok := v.(string)
	if !ok {
		return 0
	}

	f, _ := strconv.ParseFloat(s, 64)

	return f
}

// Allow implements core.Store. If Redis fails, applies FailOpen/FailClosed.
func (s *Store) Allow(key string, cost float64, l core.Limit) core.Result {
	if cost <= 0 {
		cost = 1
	}

	if l.Burst <= 0 {
		l.Burst = 1
	}

	res, err := s.run(key, cost, l)
	if err != nil {
		if s.opts.OnError != nil {
			s.opts.OnError(err)
		}

		if s.opts.FailOpen {
			return core.Result{Allowed: true, Remaining: l.Burst}
		}

		return core.Result{Allowed: false, RetryAfter: time.Second}
	}

	return res
}

// Refund implements core.Store by returning tokens (negative cost in Lua).
func (s *Store) Refund(key string, cost float64, l core.Limit) {
	if cost <= 0 {
		cost = 1
	}

	if l.Burst <= 0 {
		l.Burst = 1
	}

	_, err := s.run(key, -cost, l)
	if err != nil && s.opts.OnError != nil {
		s.opts.OnError(err)
	}
}

// Cleanup is a no-op: every key has a TTL in Redis and expires on its own.
func (s *Store) Cleanup(_ time.Duration) {}

// Len returns -1: counting keys by prefix would require a full SCAN, which is
// expensive and rarely useful. Use Redis metrics (INFO keyspace) if you
// need it.
func (s *Store) Len() int { return -1 }
