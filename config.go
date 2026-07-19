package ratelimit

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/yeferson59/goratelimit/core"
)

// Config defines the rate limiter's behavior.
type Config struct {
	// Max is the number of requests allowed per Expiration.
	// Default: 100. Ignored if you set LimitFor.
	Max int

	// Expiration is the refill window (linear refill, not a hard
	// reset). Default: 1 minute. Ignored if you set LimitFor.
	Expiration time.Duration

	// Burst is the maximum allowed burst (bucket capacity). If it's
	// 0, Burst = Max, which is the classic behavior. Setting it lower
	// than Max smooths out spikes: e.g. Max=60/min with Burst=10 allows an
	// average of 1 req/sec but never more than 10 at once. Ignored with LimitFor.
	Burst int

	// LimitFor computes the limit dynamically per request. Enables
	// limits per plan/role/route with a single middleware and a single store:
	//
	//	LimitFor: func(c *fiber.Ctx) core.Limit {
	//		if isPro(c) { return core.PerWindow(1000, time.Minute) }
	//		return core.PerWindow(100, time.Minute)
	//	}
	//
	// If nil, the fixed Max/Expiration/Burst are used.
	LimitFor func(c fiber.Ctx) core.Limit

	// Cost is how many tokens each request consumes. Default: fixed 1.
	// Useful for charging heavy endpoints more:
	//
	//	Cost: func(c *fiber.Ctx) float64 {
	//		if strings.HasPrefix(c.Path(), "/api/export") { return 10 }
	//		return 1
	//	}
	Cost func(c fiber.Ctx) float64

	// Next allows skipping the limiter for certain requests (health
	// checks, internal IPs, admins). If it returns true, no limit is
	// applied. Standard convention among Fiber middlewares.
	Next func(c fiber.Ctx) bool

	// KeyGenerator identifies the client. Default: c.IP().
	//
	// SECURITY: if your app is behind a proxy/load balancer (Railway
	// is), c.IP() returns the proxy's IP unless you configure Fiber
	// with ProxyHeader and EnableTrustedProxyCheck + TrustedProxies. Without that,
	// reading X-Forwarded-For by hand lets any client spoof
	// its identity by sending the header itself and evade the limit (or worse,
	// exhaust a victim's limit). See README, "IPs behind
	// a proxy" section.
	KeyGenerator func(c fiber.Ctx) string

	// MaxKeyLength bounds the key size. Longer keys are
	// replaced by their FNV-64 hash in hex (17 bytes), preserving
	// practical uniqueness. Prevents an attacker from inflating memory by sending
	// giant API keys/headers when the KeyGenerator reads client
	// input. Default: 128. Negative = no limit.
	MaxKeyLength int

	// MaxKeys bounds the total number of in-memory buckets; once exceeded, the
	// most idle one is evicted. This is the defense against key-flooding (millions
	// of fake identities to exhaust RAM). Default: 65536.
	MaxKeys int

	// LimitReached runs when the limit is exceeded. Default: 429 JSON.
	LimitReached fiber.Handler

	// DisableHeaders disables X-RateLimit-* and Retry-After. Exposing
	// these headers is friendly to legitimate clients, but it also
	// gives an attacker information about your thresholds; on
	// sensitive endpoints (login, password reset) it may be worth turning them off.
	DisableHeaders bool

	// SkipSuccessfulRequests refunds the token if the response was
	// < 400 (only errors/abuse count against the limit).
	SkipSuccessfulRequests bool

	// SkipFailedRequests refunds the token if the response was >= 400
	// or the handler returned an error (only successful traffic counts).
	SkipFailedRequests bool

	// Shards of the in-memory store. Default: 64.
	Shards int

	// CleanupInterval defines how often idle buckets are purged.
	// Default: 5 minutes. 0 uses the default; negative disables it.
	CleanupInterval time.Duration

	// Store allows injecting a custom backend (e.g. Redis for
	// multi-instance). If nil, the in-memory store is used.
	Store core.Store
}

func defaultConfig() Config {
	return Config{
		Max:             100,
		Expiration:      time.Minute,
		MaxKeyLength:    128,
		MaxKeys:         65536,
		Shards:          64,
		CleanupInterval: 5 * time.Minute,
		KeyGenerator: func(c fiber.Ctx) string {
			return c.IP()
		},
		LimitReached: func(c fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "too many requests",
			})
		},
	}
}

func mergeConfig(cfg ...Config) Config {
	d := defaultConfig()
	if len(cfg) == 0 {
		return d
	}
	c := cfg[0]

	if c.Max > 0 {
		d.Max = c.Max
	}

	if c.Expiration > 0 {
		d.Expiration = c.Expiration
	}

	if c.Burst > 0 {
		d.Burst = c.Burst
	}

	if c.LimitFor != nil {
		d.LimitFor = c.LimitFor
	}

	if c.Cost != nil {
		d.Cost = c.Cost
	}

	if c.Next != nil {
		d.Next = c.Next
	}

	if c.KeyGenerator != nil {
		d.KeyGenerator = c.KeyGenerator
	}

	if c.MaxKeyLength != 0 {
		d.MaxKeyLength = c.MaxKeyLength
	}

	if c.MaxKeys > 0 {
		d.MaxKeys = c.MaxKeys
	}

	if c.LimitReached != nil {
		d.LimitReached = c.LimitReached
	}

	if c.Shards > 0 {
		d.Shards = c.Shards
	}

	if c.CleanupInterval != 0 {
		d.CleanupInterval = c.CleanupInterval
	}

	d.DisableHeaders = c.DisableHeaders
	d.SkipSuccessfulRequests = c.SkipSuccessfulRequests
	d.SkipFailedRequests = c.SkipFailedRequests
	d.Store = c.Store

	return d
}

// hardenKey applies the length cap: if the key exceeds maxLen, it's
// replaced by its FNV-64 hash in hex. Keeps the key short and fixed-size
// without losing (in practice) the distinction between clients.
func hardenKey(key string, maxLen int) string {
	if maxLen < 0 || len(key) <= maxLen {
		return key
	}

	var h uint64 = 14695981039346656037

	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}

	return "h:" + strconv.FormatUint(h, 16)
}
