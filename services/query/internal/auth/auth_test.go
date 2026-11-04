package auth

import (
	"context"
	"testing"
	"time"
)

type countingLookup struct {
	calls int
	keys  map[string]string
}

func (l *countingLookup) ProjectForHash(_ context.Context, h string) (string, error) {
	l.calls++
	if pid, ok := l.keys[h]; ok {
		return pid, nil
	}
	return "", ErrUnknownKey
}

func TestHashKeyMatchesDevSeed(t *testing.T) {
	// From docs/contracts/postgres.sql.
	const want = "1cb9ec7c756e05df0214a49a974a0931046e9e7e317ec0160541b08c07bf1dea"
	if got := HashKey("tw_dev_localkey"); got != want {
		t.Fatalf("got %s", got)
	}
}

func TestCacheHitsAndExpires(t *testing.T) {
	l := &countingLookup{keys: map[string]string{HashKey("k"): "proj_a"}}
	c := NewCache(l, time.Minute)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }

	for range 3 {
		pid, err := c.ProjectID(context.Background(), "k")
		if err != nil || pid != "proj_a" {
			t.Fatalf("got %q, %v", pid, err)
		}
	}
	if l.calls != 1 {
		t.Fatalf("want 1 lookup within ttl, got %d", l.calls)
	}

	now = now.Add(61 * time.Second)
	if _, err := c.ProjectID(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if l.calls != 2 {
		t.Fatalf("want a fresh lookup after ttl, got %d calls", l.calls)
	}
}

func TestCacheUnknownKey(t *testing.T) {
	c := NewCache(&countingLookup{keys: map[string]string{}}, time.Minute)
	if _, err := c.ProjectID(context.Background(), "nope"); err != ErrUnknownKey {
		t.Fatalf("got %v", err)
	}
}
