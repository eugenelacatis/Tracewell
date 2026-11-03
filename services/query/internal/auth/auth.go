// Package auth turns an API key into a project_id, following docs/contracts/http.md.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrUnknownKey means the key doesn't exist or was revoked.
var ErrUnknownKey = errors.New("unknown api key")

// Lookup finds the project for a key hash. Postgres in production, a fake in tests.
type Lookup interface {
	ProjectForHash(ctx context.Context, keyHash string) (string, error)
}

// HashKey is the lowercase hex sha256 of the key, which is what api_keys.key_hash stores.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

type entry struct {
	projectID string
	expires   time.Time
}

// Cache remembers valid keys for ttl so most requests skip Postgres. A revoked key keeps working
// for up to ttl after revocation. Unknown keys are not cached.
type Cache struct {
	lookup Lookup
	ttl    time.Duration
	now    func() time.Time

	mu      sync.Mutex
	entries map[string]entry
}

func NewCache(lookup Lookup, ttl time.Duration) *Cache {
	return &Cache{lookup: lookup, ttl: ttl, now: time.Now, entries: map[string]entry{}}
}

// ProjectID returns the project for key, or ErrUnknownKey. Other errors mean Postgres failed.
func (c *Cache) ProjectID(ctx context.Context, key string) (string, error) {
	h := HashKey(key)
	c.mu.Lock()
	e, ok := c.entries[h]
	c.mu.Unlock()
	if ok && c.now().Before(e.expires) {
		return e.projectID, nil
	}
	pid, err := c.lookup.ProjectForHash(ctx, h)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.entries[h] = entry{projectID: pid, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return pid, nil
}

// Postgres looks keys up in the api_keys table.
type Postgres struct{ Pool *pgxpool.Pool }

func (p Postgres) ProjectForHash(ctx context.Context, keyHash string) (string, error) {
	var pid string
	err := p.Pool.QueryRow(ctx,
		`SELECT project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, keyHash,
	).Scan(&pid)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnknownKey
	}
	return pid, err
}
