// Package ratelimit is the per-project ingest limit from docs/contracts/http.md (Rate limit): a fixed
// one-second window per project, counted in Redis under rl:ingest:<project_id>:<unix second>.
package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyTTL outlives the one-second window, so a key never expires mid-window.
const keyTTL = 2 * time.Second

// Redis allows up to Limit requests per project per wall-clock second.
type Redis struct {
	Client *redis.Client
	Limit  int64
	Now    func() time.Time // time.Now when nil; tests pin it
}

// NewClient returns a client tuned for a check that sits on every ingest request. Short timeouts and
// no retries keep a dead Redis from adding latency: the check fails fast and the request fails open.
func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1,
	})
}

// Allow counts this request in the project's current window and reports whether it's within Limit.
// An error means Redis didn't answer; the caller decides what to do (ingest fails open).
func (r Redis) Allow(ctx context.Context, projectID string) (bool, error) {
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	key := "rl:ingest:" + projectID + ":" + strconv.FormatInt(now().Unix(), 10)

	// INCR and EXPIRE in one MULTI round trip, so no counter is left without a TTL.
	pipe := r.Client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, keyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return incr.Val() <= r.Limit, nil
}
