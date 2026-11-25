package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newLimiter(t *testing.T, limit int64, now *time.Time) (Redis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return Redis{Client: NewClient(mr.Addr()), Limit: limit, Now: func() time.Time { return *now }}, mr
}

func allow(t *testing.T, r Redis, project string) bool {
	t.Helper()
	ok, err := r.Allow(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestLimitPerProjectPerSecond(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	r, mr := newLimiter(t, 3, &now)

	for i := 1; i <= 3; i++ {
		if !allow(t, r, "proj_a") {
			t.Fatalf("request %d under the limit was rejected", i)
		}
	}
	if allow(t, r, "proj_a") {
		t.Fatal("4th request in the same second was allowed")
	}
	if !allow(t, r, "proj_b") {
		t.Fatal("another project shares proj_a's counter")
	}

	key := "rl:ingest:proj_a:1800000000"
	if got, _ := mr.Get(key); got != "4" {
		t.Fatalf("%s = %q, want 4", key, got)
	}
	if ttl := mr.TTL(key); ttl != keyTTL {
		t.Fatalf("ttl = %v, want %v", ttl, keyTTL)
	}

	now = now.Add(time.Second)
	if !allow(t, r, "proj_a") {
		t.Fatal("the next second didn't start a fresh window")
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	now := time.Now()
	r, mr := newLimiter(t, 3, &now)
	mr.Close()

	start := time.Now()
	if _, err := r.Allow(context.Background(), "proj_a"); err == nil {
		t.Fatal("expected an error with Redis down")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("a down Redis took %v to fail; the check sits on every request", d)
	}
}
