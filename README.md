# Go Rate Limit for Fiber

Rate limiter for [Fiber](https://gofiber.io) built from scratch: token bucket
with partitioned in-memory storage, dynamic per-request limits, variable
cost, and bounded memory against key-flooding attacks.

Measured in sandbox (Intel Xeon 2.10GHz, `go test -bench -benchmem`):
**~114 ns/op, 0 B/op, 0 allocs/op** on the hot path, with `-race` clean.

## Installation

```bash
go get github.com/yeferson59/fiber-ratelimit
go mod tidy
```

## Basic usage

```go
app.Use(ratelimit.New(ratelimit.Config{
	Max:        100,
	Expiration: time.Minute,
}))
```

## Customization

### Burst separate from the sustained rate

`Max/Expiration` defines the average; `Burst` how much is allowed at once:

```go
// Average 60/min (1/sec), but never more than 10 instantaneous requests.
ratelimit.New(ratelimit.Config{
	Max: 60, Expiration: time.Minute, Burst: 10,
})
```

### Dynamic limits per plan/role (a single middleware, a single store)

```go
ratelimit.New(ratelimit.Config{
	KeyGenerator: func(c *fiber.Ctx) string {
		if id := c.Locals("userID"); id != nil {
			return "u:" + id.(string)
		}
		return "ip:" + c.IP()
	},
	LimitFor: func(c *fiber.Ctx) core.Limit {
		switch c.Locals("plan") {
		case "pro":
			return core.PerWindow(1000, time.Minute)
		default:
			return core.PerWindow(100, time.Minute)
		}
	},
})
```

### Variable cost per endpoint

Heavy endpoints consume more tokens from the same budget:

```go
Cost: func(c *fiber.Ctx) float64 {
	if strings.HasPrefix(c.Path(), "/api/export") { return 10 }
	return 1
},
```

### Skipping the limiter (health checks, internal IPs)

```go
Next: func(c *fiber.Ctx) bool {
	return c.Path() == "/health"
},
```

### Counting only what matters to you

- `SkipSuccessfulRequests: true` — refunds responses < 400; only
  errors consume the limit (useful against brute force on login: successful
  attempts don't get penalized).
- `SkipFailedRequests: true` — refunds responses >= 400; only successful
  traffic consumes the limit.

### Headers

By default it exposes `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Reset` (epoch), and `Retry-After` on rejection. With
`DisableHeaders: true` they're hidden — recommended on sensitive endpoints
(login, password reset) to avoid revealing your thresholds to an attacker.

## Security

### IPs behind a proxy (Railway, nginx, Cloudflare)

The default limits by `c.IP()`. Behind a proxy, that's the proxy's IP
(all requests share a bucket) unless you configure Fiber to trust the
correct header:

```go
app := fiber.New(fiber.Config{
	ProxyHeader:             "X-Forwarded-For",
	EnableTrustedProxyCheck: true,
	TrustedProxies:          []string{"10.0.0.0/8"}, // your proxy's range
})
```

Never read `X-Forwarded-For` by hand in the `KeyGenerator` without
validating the proxy: any client can send that header and spoof its
identity — evading its own limit or exhausting a victim's. With
`EnableTrustedProxyCheck`, Fiber only honors the header when the
connection comes from a proxy in your list.

### Key-flooding (memory exhaustion)

An attacker who fabricates millions of identities (spoofed IPs behind a
misconfigured proxy, random API keys) can inflate an unbounded store until
it takes down the process. Defenses included:

- `MaxKeys` (default 65536): global cap on buckets; once exceeded, the
  most idle one in the affected shard is evicted. Memory stays bounded to
  a few MB.
- `MaxKeyLength` (default 128): longer keys collapse to their FNV-64
  hash, so a giant header doesn't inflate memory per entry.
- `CleanupInterval` (default 5 min): periodic purge of idle buckets.

Eviction has a deliberate cost: if the store is full, inserting a new
key scans its shard — O(n/shards) only under attack, in exchange for
constant memory. Legitimate traffic within `MaxKeys` never pays for it.

### Fail-open vs fail-closed

The in-memory store can't "fail." If you implement a `core.Store`
backed by Redis, decide what to do when Redis doesn't respond: allow
(fail-open, prioritizes availability) or reject (fail-closed, prioritizes
protection). For a general traffic limiter, fail-open is usually
correct; for login/payments, consider fail-closed.

## Multi-instance: RedisStore

The in-memory store is per instance: with N containers the effective
limit is `Max × N`. To share a single count across instances, use the
`redisstore` subpackage:

```go
import (
	"github.com/redis/go-redis/v9"
	ratelimit "github.com/yeferson59/fiber-ratelimit"
	"github.com/yeferson59/fiber-ratelimit/redisstore"
)

rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})

app.Use(ratelimit.New(ratelimit.Config{
	Max:        100,
	Expiration: time.Minute,
	Store: redisstore.New(rdb, redisstore.Options{
		Prefix:   "rl:",
		Timeout:  50 * time.Millisecond,
		FailOpen: true, // if Redis goes down, allow (fail-closed for login/payments)
		OnError:  func(err error) { log.Printf("ratelimit redis: %v", err) },
	}),
}))
```

How it works internally:

- All the token bucket logic lives in **a single Lua script** that Redis
  executes atomically (read, refill, consume, set TTL in one
  operation). A separate GET + SET would let two instances read the
  same balance and both consume — the script eliminates that race.
- **Redis provides the time** (the `TIME` command inside the script), not
  each container: desynchronized clocks between instances don't
  corrupt the count.
- Each key carries a computed **TTL** (time until fully refilled plus
  margin): Redis cleans up on its own, `Cleanup` is a no-op.
- **Short timeout** (50ms default) + `FailOpen`/`FailClosed`: a degraded
  Redis doesn't add unbounded latency to every request or bring down
  your API.
- `redisstore.New` accepts `*redis.Client`, `*redis.ClusterClient`, or
  any `redis.Scripter`.

If you don't import `redisstore`, your binary doesn't include go-redis.

### Measured cost

| Store  | Latency (sandbox)     | Notes                                     |
| ------ | --------------------- | ----------------------------------------- |
| Memory | ~114 ns/op, 0 alloc   | per instance                              |
| Redis  | ~33 µs/op (localhost) | shared; expect 0.5–2 ms on a real network |

Rule of thumb: memory while you have 1 instance; Redis only when you
scale horizontally and a shared count actually matters.

## Structure

```
fiber-ratelimit/
├── core/
│   ├── store.go       # algorithm + sharding + eviction, no Fiber dependency
│   └── store_test.go  # 10 tests + benchmark
├── redisstore/
│   ├── store.go       # core.Store over Redis (atomic Lua script)
│   └── store_test.go  # 7 tests against real Redis + fail-open/closed + benchmark
├── config.go          # Config, defaults, key hardening
├── middleware.go      # the fiber.Handler
├── middleware_test.go # 7 integration tests with httptest
└── go.mod
```

## Running the tests

```bash
go test -race ./...                       # redisstore is skipped if there's no local Redis
go test -bench=. -benchmem -run=^$ ./core/... ./redisstore/...
```

The `redisstore` tests expect Redis on `localhost:6379` and run
`FLUSHDB` — use a disposable instance (`docker run -p 6379:6379 redis`).
If there's no Redis, they skip themselves (`t.Skip`).

## Possible next steps

- Prometheus metrics: allowed/rejected per route, store size
  (`Store.Len()` already exists for this).
- Draft RFC "RateLimit header fields for HTTP" if you want the
  standardized headers (`RateLimit-Policy`, etc.) instead of the `X-` ones.
- Local fallback: use the in-memory store as backup when Redis goes
  down, instead of pure fail-open (approximate limit > no limit).
