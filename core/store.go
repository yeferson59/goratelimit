// Package core contains the rate limiter engine: token bucket with
// sharded in-memory storage. It has no dependency on Fiber so it can be
// tested in isolation and swapped for a distributed backend
// (Redis, etc.) without touching the middleware.
package core

import (
	"sync"
	"time"
)

// Limit describes how many tokens are refilled and what the maximum
// capacity of the bucket is. It's passed on each call to Allow, so the same
// store can apply different limits to different keys (by plan, by route, etc.).
type Limit struct {
	// Rate is the refill speed in tokens per second.
	Rate float64
	// Burst is the maximum capacity of the bucket (allowed burst).
	// If <= 0 it's treated as 1.
	Burst float64
}

// PerWindow builds a classic Limit: max requests per window, with
// burst equal to the max. Equivalent to v1 behavior.
func PerWindow(maxRequests int, window time.Duration) Limit {
	return Limit{
		Rate:  float64(maxRequests) / window.Seconds(),
		Burst: float64(maxRequests),
	}
}

// Result is the result of a call to Allow.
type Result struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration // how long to wait for the requested cost to be available
	ResetAfter time.Duration // how long until the bucket is full again
}

// Store abstracts the backend that holds each bucket's state.
type Store interface {
	// Allow attempts to consume `cost` tokens for `key` under limit `l`.
	Allow(key string, cost float64, l Limit) Result

	// Refund returns `cost` tokens to `key` (for requests you decided to
	// forgive, e.g. successful responses or your own 5xx errors).
	Refund(key string, cost float64, l Limit)

	// Cleanup removes buckets that have been idle for longer than idleFor.
	Cleanup(idleFor time.Duration)

	// Len returns the approximate number of live buckets (for metrics).
	Len() int
}

type tokenBucket struct {
	mu        sync.Mutex
	tokens    float64
	lastCheck int64 // UnixNano
}

// shard groups a subset of buckets under its own lock, so that
// requests for different keys don't block each other.
type shard struct {
	mu         sync.RWMutex
	buckets    map[string]*tokenBucket
	maxBuckets int
}

type memoryStore struct {
	shards []*shard
	mask   uint64
}

// MemoryStoreOptions configures the in-memory store.
type MemoryStoreOptions struct {
	// Shards is the number of partitions. Rounded up to the next
	// power of 2. Default: 64.
	Shards int
	// MaxKeys is the approximate global cap on in-memory buckets. Once
	// exceeded, the most idle bucket in the affected shard is evicted.
	// This bounds memory against key-flooding attacks. Default: 65536.
	// At ~64 bytes per bucket plus map overhead, the default stays
	// within a few MB.
	MaxKeys int
}

// NewMemoryStore creates an in-memory Store with the given options.
func NewMemoryStore(opts MemoryStoreOptions) Store {
	n := opts.Shards
	if n < 1 {
		n = 64
	}

	if n&(n-1) != 0 {
		p := 1

		for p < n {
			p <<= 1
		}

		n = p
	}

	maxKeys := opts.MaxKeys

	if maxKeys <= 0 {
		maxKeys = 65536
	}

	perShard := max(1, maxKeys/n)
	shards := make([]*shard, n)

	for i := range shards {
		shards[i] = new(shard{
			buckets:    make(map[string]*tokenBucket),
			maxBuckets: perShard,
		})
	}

	return new(memoryStore{shards, uint64(n - 1)})
}

// hashKey is 64-bit FNV-1a: no dependencies, fast, and with good
// distribution for choosing a shard.
func hashKey(key string) uint64 {
	var h uint64 = 14695981039346656037

	for i := range len(key) {
		h ^= uint64(key[i])
		h *= 1099511628211
	}

	return h
}

func (s *memoryStore) getShard(key string) *shard {
	return s.shards[hashKey(key)&s.mask]
}

// refill updates the bucket's tokens based on elapsed time.
// Must be called with b.mu held.
func refill(b *tokenBucket, l Limit, now int64) {
	elapsed := now - b.lastCheck
	b.lastCheck = now

	b.tokens += (float64(elapsed) / 1e9) * l.Rate

	if b.tokens > l.Burst {
		b.tokens = l.Burst
	}
}

func normalize(l Limit) Limit {
	if l.Burst <= 0 {
		l.Burst = 1
	}

	if l.Rate < 0 {
		l.Rate = 0
	}

	return l
}

func (s *memoryStore) Allow(key string, cost float64, l Limit) Result {
	l = normalize(l)
	if cost <= 0 {
		cost = 1
	}
	sh := s.getShard(key)

	// Fast path: most requests are for already-seen keys; the
	// RLock allows concurrent reads in the same shard.
	sh.mu.RLock()
	b, ok := sh.buckets[key]
	sh.mu.RUnlock()

	if !ok {
		b = s.insert(sh, key, l)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UnixNano()
	refill(b, l, now)

	if b.tokens < cost {
		var wait time.Duration

		if l.Rate > 0 {
			wait = time.Duration((cost - b.tokens) / l.Rate * 1e9)
		} else {
			wait = time.Duration(1<<63 - 1) // rate 0: never refills
		}

		return Result{
			Allowed:    false,
			Remaining:  b.tokens,
			RetryAfter: wait,
			ResetAfter: resetAfter(b.tokens, l),
		}
	}

	b.tokens -= cost

	return Result{
		Allowed:    true,
		Remaining:  b.tokens,
		ResetAfter: resetAfter(b.tokens, l),
	}
}

func resetAfter(tokens float64, l Limit) time.Duration {
	if l.Rate <= 0 || tokens >= l.Burst {
		return 0
	}

	return time.Duration((l.Burst - tokens) / l.Rate * 1e9)
}

// insert creates the bucket for key, evicting the most idle one in the shard if
// the cap has been reached. Eviction is O(n) over the shard, but only occurs
// when full — which is exactly the attack scenario, where we'd
// rather pay bounded CPU than unbounded memory.
func (s *memoryStore) insert(sh *shard, key string, l Limit) *tokenBucket {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Double check: another goroutine may have created it while we waited.
	if b, ok := sh.buckets[key]; ok {
		return b
	}

	if len(sh.buckets) >= sh.maxBuckets {
		var oldestKey string

		oldest := int64(1<<63 - 1)

		for k, b := range sh.buckets {
			b.mu.Lock()
			lc := b.lastCheck
			b.mu.Unlock()

			if lc < oldest {
				oldest = lc
				oldestKey = k
			}
		}

		delete(sh.buckets, oldestKey)
	}

	b := new(tokenBucket{tokens: l.Burst, lastCheck: time.Now().UnixNano()})

	sh.buckets[key] = b

	return b
}

func (s *memoryStore) Refund(key string, cost float64, l Limit) {
	l = normalize(l)

	if cost <= 0 {
		cost = 1
	}

	sh := s.getShard(key)

	sh.mu.RLock()
	b, ok := sh.buckets[key]
	sh.mu.RUnlock()

	if !ok {
		return
	}

	b.mu.Lock()
	b.tokens += cost

	if b.tokens > l.Burst {
		b.tokens = l.Burst
	}

	b.mu.Unlock()
}

func (s *memoryStore) Cleanup(idleFor time.Duration) {
	cutoff := time.Now().Add(-idleFor).UnixNano()

	for _, sh := range s.shards {
		sh.mu.Lock()

		for k, b := range sh.buckets {
			b.mu.Lock()
			stale := b.lastCheck < cutoff
			b.mu.Unlock()

			if stale {
				delete(sh.buckets, k)
			}
		}

		sh.mu.Unlock()
	}
}

func (s *memoryStore) Len() int {
	var total int

	for _, sh := range s.shards {
		sh.mu.RLock()
		total += len(sh.buckets)
		sh.mu.RUnlock()
	}

	return total
}
