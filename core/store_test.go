package core

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAllowWithinLimit(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := PerWindow(5, time.Second)

	for i := range 5 {
		if r := st.Allow("client-a", 1, l); !r.Allowed {
			t.Fatalf("request %d should be allowed within the limit", i+1)
		}
	}

	r := st.Allow("client-a", 1, l)
	if r.Allowed {
		t.Fatalf("the 6th request should be rejected")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("RetryAfter should be positive, got %v", r.RetryAfter)
	}
	if r.ResetAfter <= 0 {
		t.Fatalf("ResetAfter should be positive, got %v", r.ResetAfter)
	}
}

func TestDynamicLimitsPerKey(t *testing.T) {
	// A single store, different limits per key: the basis for per-plan limits.
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	free := PerWindow(1, time.Second)
	pro := PerWindow(10, time.Second)

	if r := st.Allow("user-free", 1, free); !r.Allowed {
		t.Fatal("first request from user-free should pass")
	}
	if r := st.Allow("user-free", 1, free); r.Allowed {
		t.Fatal("second request from user-free should be rejected")
	}

	for i := range 10 {
		if r := st.Allow("user-pro", 1, pro); !r.Allowed {
			t.Fatalf("request %d from user-pro should pass with the higher limit", i+1)
		}
	}
}

func TestWeightedCost(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := PerWindow(10, time.Second)

	if r := st.Allow("client-a", 7, l); !r.Allowed {
		t.Fatal("cost 7 with 10 tokens should pass")
	}
	if r := st.Allow("client-a", 5, l); r.Allowed {
		t.Fatal("cost 5 with ~3 tokens remaining should be rejected")
	}
	if r := st.Allow("client-a", 3, l); !r.Allowed {
		t.Fatal("cost 3 with ~3 tokens remaining should pass")
	}
}

func TestBurstSeparateFromRate(t *testing.T) {
	// Low sustained rate (2/sec) but burst of 6: the first 6 pass
	// at once, the seventh doesn't.
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := Limit{Rate: 2, Burst: 6}

	for i := range 6 {
		if r := st.Allow("client-a", 1, l); !r.Allowed {
			t.Fatalf("burst: request %d should pass", i+1)
		}
	}
	if r := st.Allow("client-a", 1, l); r.Allowed {
		t.Fatal("request 7 should be rejected: burst exhausted")
	}

	// At 2 tokens/sec, after ~600ms there's at least 1 token again.
	time.Sleep(600 * time.Millisecond)
	if r := st.Allow("client-a", 1, l); !r.Allowed {
		t.Fatal("after partial refill there should be at least 1 token")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := PerWindow(1, time.Second)

	st.Allow("client-a", 1, l)
	if r := st.Allow("client-a", 1, l); r.Allowed {
		t.Fatal("client-a should have no more tokens")
	}
	if r := st.Allow("client-b", 1, l); !r.Allowed {
		t.Fatal("client-b should not be affected by client-a")
	}
}

func TestRefund(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := PerWindow(1, time.Second)

	st.Allow("client-a", 1, l)
	if r := st.Allow("client-a", 1, l); r.Allowed {
		t.Fatal("second attempt should be rejected")
	}
	st.Refund("client-a", 1, l)
	if r := st.Allow("client-a", 1, l); !r.Allowed {
		t.Fatal("after Refund there should be a token available")
	}
}

func TestMaxKeysEviction(t *testing.T) {
	// 1 shard with a cap of 8 keys: inserting the ninth doesn't grow the
	// total, and the oldest key was evicted.
	st := NewMemoryStore(MemoryStoreOptions{Shards: 1, MaxKeys: 8})
	l := PerWindow(10, time.Second)

	for i := range 8 {
		st.Allow(fmt.Sprintf("key-%d", i), 1, l)
		time.Sleep(2 * time.Millisecond) // ensure a distinct lastCheck per key
	}
	if got := st.Len(); got != 8 {
		t.Fatalf("expected 8 buckets, got %d", got)
	}

	st.Allow("key-new", 1, l)
	if got := st.Len(); got != 8 {
		t.Fatalf("memory should stay bounded at 8, got %d", got)
	}

	// key-0 (the most idle) should have been evicted: requesting it again
	// recreates it with a full bucket, a sign it no longer existed.
	r := st.Allow("key-0", 1, l)
	if !r.Allowed || int(r.Remaining) != 9 {
		t.Fatalf("key-0 should have been recreated full; remaining=%v", r.Remaining)
	}
}

func TestCleanupRemovesIdleBuckets(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := PerWindow(3, time.Second)

	st.Allow("client-a", 1, l)
	time.Sleep(60 * time.Millisecond)
	st.Cleanup(30 * time.Millisecond)

	if got := st.Len(); got != 0 {
		t.Fatalf("the idle bucket should have been removed, %d remain", got)
	}
}

func TestZeroRateNeverRefills(t *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 4})
	l := Limit{Rate: 0, Burst: 1}

	if r := st.Allow("client-a", 1, l); !r.Allowed {
		t.Fatal("first request should consume the only token")
	}
	r := st.Allow("client-a", 1, l)
	if r.Allowed {
		t.Fatal("with rate 0 there should be no refill")
	}
	if r.RetryAfter <= 0 {
		t.Fatal("RetryAfter should be huge/positive with rate 0")
	}
}

func TestConcurrentAccessIsSafe(_ *testing.T) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 32})
	l := PerWindow(1_000_000, time.Second)
	var wg sync.WaitGroup

	for range 50 {
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			for range 100 {
				st.Allow("shared-key", 1, l)
			}
		})
	}
	wg.Wait()
}

func BenchmarkAllowParallel(b *testing.B) {
	st := NewMemoryStore(MemoryStoreOptions{Shards: 64})
	l := PerWindow(1_000_000_000, time.Second)
	keys := make([]string, 26)
	for i := range keys {
		keys[i] = "client-" + string(rune('a'+i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			st.Allow(keys[i%len(keys)], 1, l)
			i++
		}
	})
}
