package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/yeferson59/goratelimit/core"
)

func fixedKey(name string) func(c fiber.Ctx) string {
	return func(_ fiber.Ctx) string { return name }
}

func doReq(t *testing.T, app *fiber.App, path string, headers map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp
}

func TestBlocksOverLimitWithHeaders(t *testing.T) {
	app := fiber.New()
	app.Use(New(Config{Max: 2, Expiration: time.Second, KeyGenerator: fixedKey("k")}))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	doReq(t, app, "/", nil)
	doReq(t, app, "/", nil)
	resp := doReq(t, app, "/", nil)

	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	for _, h := range []string{"Retry-After", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"} {
		if resp.Header.Get(h) == "" {
			t.Fatalf("expected header %s to be present", h)
		}
	}
}

func TestDisableHeaders(t *testing.T) {
	app := fiber.New()
	app.Use(New(Config{Max: 1, Expiration: time.Second, DisableHeaders: true, KeyGenerator: fixedKey("k")}))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	doReq(t, app, "/", nil)
	resp := doReq(t, app, "/", nil)

	if resp.StatusCode != fiber.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-RateLimit-Limit") != "" || resp.Header.Get("Retry-After") != "" {
		t.Fatal("with DisableHeaders there should be no rate limit headers")
	}
}

func TestNextSkips(t *testing.T) {
	app := fiber.New()
	app.Use(New(Config{
		Max: 1, Expiration: time.Second,
		KeyGenerator: fixedKey("k"),
		Next: func(c fiber.Ctx) bool {
			return c.Path() == "/health"
		},
	}))
	app.Get("/health", func(c fiber.Ctx) error { return c.SendString("ok") })

	for i := range 5 {
		if resp := doReq(t, app, "/health", nil); resp.StatusCode != 200 {
			t.Fatalf("health check %d should not be limited, got %d", i+1, resp.StatusCode)
		}
	}
}

func TestLimitForDynamicByHeader(t *testing.T) {
	// Simulates plans: the X-Plan header decides the limit. The key includes
	// the plan so each "user" has its own bucket.
	app := fiber.New()
	app.Use(New(Config{
		KeyGenerator: func(c fiber.Ctx) string { return c.Get("X-Plan") },
		LimitFor: func(c fiber.Ctx) core.Limit {
			if c.Get("X-Plan") == "pro" {
				return core.PerWindow(5, time.Second)
			}
			return core.PerWindow(1, time.Second)
		},
	}))
	app.Get("/", func(c fiber.Ctx) error { return c.SendString("ok") })

	free := map[string]string{"X-Plan": "free"}
	pro := map[string]string{"X-Plan": "pro"}

	doReq(t, app, "/", free)
	if resp := doReq(t, app, "/", free); resp.StatusCode != 429 {
		t.Fatalf("free should exhaust its limit of 1, got %d", resp.StatusCode)
	}
	for i := range 5 {
		if resp := doReq(t, app, "/", pro); resp.StatusCode != 200 {
			t.Fatalf("pro request %d should pass, got %d", i+1, resp.StatusCode)
		}
	}
	if resp := doReq(t, app, "/", pro); resp.StatusCode != 429 {
		t.Fatalf("pro should exhaust its limit of 5, got %d", resp.StatusCode)
	}
}

func TestWeightedCostByPath(t *testing.T) {
	app := fiber.New()
	app.Use(New(Config{
		Max:          10,
		Expiration:   time.Second,
		KeyGenerator: fixedKey("k"),
		Cost: func(c fiber.Ctx) float64 {
			if c.Path() == "/export" {
				return 10
			}

			return 1
		},
	}))
	app.Get("/export", func(c fiber.Ctx) error { return c.SendString("heavy") })
	app.Get("/light", func(c fiber.Ctx) error { return c.SendString("light") })

	if resp := doReq(t, app, "/export", nil); resp.StatusCode != 200 {
		t.Fatalf("export with a full bucket should pass, got %d", resp.StatusCode)
	}
	if resp := doReq(t, app, "/light", nil); resp.StatusCode != 429 {
		t.Fatalf("after export (cost 10) no tokens should remain, got %d", resp.StatusCode)
	}
}

func TestSkipFailedRequestsRefunds(t *testing.T) {
	app := fiber.New()
	app.Use(New(Config{
		Max: 1, Expiration: time.Minute,
		KeyGenerator:       fixedKey("k"),
		SkipFailedRequests: true,
	}))
	app.Get("/notfound", func(c fiber.Ctx) error {
		return c.SendStatus(fiber.StatusNotFound)
	})
	app.Get("/ok", func(c fiber.Ctx) error { return c.SendString("ok") })

	// 404s are refunded: they don't consume the limit.
	for range 3 {
		if resp := doReq(t, app, "/notfound", nil); resp.StatusCode != 404 {
			t.Fatalf("expected 404, got %d", resp.StatusCode)
		}
	}
	// The token is still available for a successful one.
	if resp := doReq(t, app, "/ok", nil); resp.StatusCode != 200 {
		t.Fatalf("the token should still be available, got %d", resp.StatusCode)
	}
	// And now it's exhausted.
	if resp := doReq(t, app, "/ok", nil); resp.StatusCode != 429 {
		t.Fatalf("the limit should be exhausted, got %d", resp.StatusCode)
	}
}

func TestLongKeysAreHashedNotUnbounded(t *testing.T) {
	// Verifies the hardening directly on hardenKey.
	long := make([]byte, 10_000)
	for i := range long {
		long[i] = 'a'
	}
	k1 := hardenKey(string(long), 128)
	if len(k1) > 32 {
		t.Fatalf("long key should collapse to a short hash, length %d", len(k1))
	}
	long[9999] = 'b'
	k2 := hardenKey(string(long), 128)
	if k1 == k2 {
		t.Fatal("different keys should produce different hashes")
	}
	if hardenKey("short", 128) != "short" {
		t.Fatal("short keys should remain unchanged")
	}
}
