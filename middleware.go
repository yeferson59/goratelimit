// Package ratelimit is a rate limiter for Fiber based on the token bucket
// algorithm, with partitioned in-memory storage, dynamic per-request limits,
// variable cost, and bounded memory against key-flooding. See README.md.
package ratelimit

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/yeferson59/goratelimit/core"
)

// New creates the rate limiting middleware for Fiber.
//
//	app.Use(ratelimit.New(ratelimit.Config{
//		Max:        100,
//		Expiration: time.Minute,
//	}))
func New(cfg ...Config) fiber.Handler {
	c := mergeConfig(cfg...)

	st := c.Store
	if st == nil {
		st = core.NewMemoryStore(core.MemoryStoreOptions{
			Shards:  c.Shards,
			MaxKeys: c.MaxKeys,
		})
	}

	// Precomputed static limit; only used if there's no LimitFor.
	staticLimit := core.PerWindow(c.Max, c.Expiration)
	if c.Burst > 0 {
		staticLimit.Burst = float64(c.Burst)
	}

	if c.CleanupInterval > 0 {
		go func() {
			ticker := time.NewTicker(c.CleanupInterval)
			defer ticker.Stop()
			for range ticker.C {
				// A bucket with no activity for 2 windows is already
				// full again; removing it is equivalent to recreating it.
				st.Cleanup(c.Expiration * 2)
			}
		}()
	}

	refundable := c.SkipSuccessfulRequests || c.SkipFailedRequests

	return func(ctx fiber.Ctx) error {
		if c.Next != nil && c.Next(ctx) {
			return ctx.Next()
		}

		key := hardenKey(c.KeyGenerator(ctx), c.MaxKeyLength)

		limit := staticLimit
		if c.LimitFor != nil {
			limit = c.LimitFor(ctx)
		}

		cost := 1.0
		if c.Cost != nil {
			cost = c.Cost(ctx)
		}

		res := st.Allow(key, cost, limit)

		if !c.DisableHeaders {
			ctx.Set("X-RateLimit-Limit", strconv.Itoa(int(limit.Burst)))
			ctx.Set("X-RateLimit-Remaining", strconv.Itoa(int(res.Remaining)))
			ctx.Set("X-RateLimit-Reset", strconv.Itoa(int(time.Now().Add(res.ResetAfter).Unix())))
		}

		if !res.Allowed {
			if !c.DisableHeaders {
				secs := int(res.RetryAfter.Seconds()) + 1

				ctx.Set("Retry-After", strconv.Itoa(secs))
			}

			return c.LimitReached(ctx)
		}

		if !refundable {
			return ctx.Next()
		}

		err := ctx.Next()
		status := ctx.Response().StatusCode()
		succeeded := err == nil && status < 400

		if (c.SkipSuccessfulRequests && succeeded) ||
			(c.SkipFailedRequests && !succeeded) {
			st.Refund(key, cost, limit)
		}

		return err
	}
}
