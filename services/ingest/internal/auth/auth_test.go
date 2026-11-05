package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeLookup struct {
	keys  map[string]string // hash -> project
	calls int
}

func (f *fakeLookup) ProjectForHash(_ context.Context, h string) (string, error) {
	f.calls++
	if pid, ok := f.keys[h]; ok {
		return pid, nil
	}
	return "", ErrUnknownKey
}

func TestHashKeyMatchesDevSeed(t *testing.T) {
	// The hash stored for tw_dev_localkey in docs/contracts/postgres.sql.
	const want = "1cb9ec7c756e05df0214a49a974a0931046e9e7e317ec0160541b08c07bf1dea"
	if got := HashKey("tw_dev_localkey"); got != want {
		t.Fatalf("HashKey = %s, want %s", got, want)
	}
}

func TestCacheHitsLookupOncePerTTL(t *testing.T) {
	f := &fakeLookup{keys: map[string]string{HashKey("good"): "proj_a"}}
	c := NewCache(f, time.Minute)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }

	for range 3 {
		pid, err := c.ProjectID(context.Background(), "good")
		if err != nil || pid != "proj_a" {
			t.Fatalf("ProjectID = %q, %v", pid, err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("lookups = %d, want 1", f.calls)
	}
	now = now.Add(61 * time.Second)
	if _, err := c.ProjectID(context.Background(), "good"); err != nil {
		t.Fatal(err)
	}
	if f.calls != 2 {
		t.Fatalf("lookups after ttl = %d, want 2", f.calls)
	}
}

func TestCacheDoesNotCacheUnknownKeys(t *testing.T) {
	f := &fakeLookup{keys: map[string]string{}}
	c := NewCache(f, time.Minute)
	for range 2 {
		if _, err := c.ProjectID(context.Background(), "bad"); !errors.Is(err, ErrUnknownKey) {
			t.Fatalf("err = %v, want ErrUnknownKey", err)
		}
	}
	if f.calls != 2 {
		t.Fatalf("lookups = %d, want 2", f.calls)
	}
}
