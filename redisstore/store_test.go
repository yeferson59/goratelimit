package redisstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yeferson59/goratelimit/core"
)

// newTestClient connects to the local Redis; if unavailable, the test is skipped.
func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "password"})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available at localhost:6379: %v", err)
	}
	// Unique prefix per test to isolate keys.
	c.FlushDB(context.Background())
	return c
}

func TestAllowWithinLimit(t *testing.T) {
	st := New(newTestClient(t), Options{})
	l := core.PerWindow(5, time.Second)

	for i := range 5 {
		if r := st.Allow("client-a", 1, l); !r.Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	r := st.Allow("client-a", 1, l)
	if r.Allowed {
		t.Fatal("the 6th request should be rejected")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("RetryAfter should be positive, got %v", r.RetryAfter)
	}
}

func TestSharedStateAcrossInstances(t *testing.T) {
	// Two different Stores (simulating two containers) over the same
	// Redis must share the count: that's the whole point of the package.
	client := newTestClient(t)
	instanceA := New(client, Options{})
	instanceB := New(client, Options{})
	l := core.PerWindow(3, time.Second)

	instanceA.Allow("client-x", 1, l)
	instanceB.Allow("client-x", 1, l)
	instanceA.Allow("client-x", 1, l)

	if r := instanceB.Allow("client-x", 1, l); r.Allowed {
		t.Fatal("the 4th request should be rejected even coming from another instance")
	}
}

func TestWeightedCostAndRefund(t *testing.T) {
	st := New(newTestClient(t), Options{})
	l := core.PerWindow(10, time.Second)

	if r := st.Allow("client-a", 7, l); !r.Allowed {
		t.Fatal("cost 7 with 10 tokens should pass")
	}
	if r := st.Allow("client-a", 5, l); r.Allowed {
		t.Fatal("cost 5 with ~3 remaining should be rejected")
	}

	st.Refund("client-a", 7, l)

	if r := st.Allow("client-a", 5, l); !r.Allowed {
		t.Fatal("after refunding 7, cost 5 should pass")
	}
}

func TestRefillOverTime(t *testing.T) {
	st := New(newTestClient(t), Options{})
	l := core.PerWindow(2, 200*time.Millisecond)

	st.Allow("client-a", 1, l)
	st.Allow("client-a", 1, l)
	if r := st.Allow("client-a", 1, l); r.Allowed {
		t.Fatal("no tokens should remain")
	}

	time.Sleep(250 * time.Millisecond)

	if r := st.Allow("client-a", 1, l); !r.Allowed {
		t.Fatal("after the full window it should have refilled")
	}
}

func TestKeyHasTTL(t *testing.T) {
	client := newTestClient(t)
	st := New(client, Options{Prefix: "rl:"})
	l := core.PerWindow(5, time.Second)

	st.Allow("client-ttl", 1, l)

	ttl, err := client.TTL(context.Background(), "rl:client-ttl").Result()
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("the key should have a positive TTL so it doesn't accumulate, got %v", ttl)
	}
}

// brokenScripter simulates a downed Redis to test fail-open/fail-closed.
type brokenScripter struct{}

func (brokenScripter) Eval(ctx context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func (brokenScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func (brokenScripter) EvalRO(ctx context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func (brokenScripter) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func (brokenScripter) ScriptExists(ctx context.Context, _ ...string) *redis.BoolSliceCmd {
	cmd := redis.NewBoolSliceCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func (brokenScripter) ScriptLoad(ctx context.Context, _ string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(errors.New("redis down (simulated)"))
	return cmd
}

func TestFailOpenAllowsWhenRedisDown(t *testing.T) {
	errCount := 0
	st := New(brokenScripter{}, Options{
		FailOpen: true,
		OnError:  func(_ error) { errCount++ },
	})
	l := core.PerWindow(1, time.Second)

	if r := st.Allow("client-a", 1, l); !r.Allowed {
		t.Fatal("with FailOpen, a downed Redis should allow the request")
	}
	if errCount == 0 {
		t.Fatal("OnError should have been invoked")
	}
}

func TestFailClosedRejectsWhenRedisDown(t *testing.T) {
	st := New(brokenScripter{}, Options{FailOpen: false})
	l := core.PerWindow(100, time.Second)

	if r := st.Allow("client-a", 1, l); r.Allowed {
		t.Fatal("with FailClosed, a downed Redis should reject the request")
	}
}

func BenchmarkAllowRedis(b *testing.B) {
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "password"})
	if err := c.Ping(context.Background()).Err(); err != nil {
		b.Skipf("redis not available: %v", err)
	}
	st := New(c, Options{})
	l := core.PerWindow(1_000_000_000, time.Second)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			st.Allow("bench-key", 1, l)
		}
	})
}
