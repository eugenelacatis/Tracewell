//go:build integration

package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Runs the real api_keys query against the compose Postgres (docs/contracts/postgres.sql applied).
// go test -tags integration ./internal/auth/
func TestPostgresGoodBadAndRevokedKeys(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // registered first, so it runs after the row cleanup below
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres not reachable: %v", err)
	}

	project := fmt.Sprintf("proj_authtest_%d", time.Now().UnixNano())
	active, revoked := project+"_active", project+"_revoked"
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO projects (id, org_id, name) VALUES ($1, 'org_dev', 'auth test')`, project)
	exec(`INSERT INTO api_keys (project_id, name, key_prefix, key_hash) VALUES ($1, 'a', 'x', $2)`, project, HashKey(active))
	exec(`INSERT INTO api_keys (project_id, name, key_prefix, key_hash, revoked_at) VALUES ($1, 'r', 'x', $2, now())`,
		project, HashKey(revoked))
	t.Cleanup(func() {
		exec(`DELETE FROM api_keys WHERE project_id = $1`, project)
		exec(`DELETE FROM projects WHERE id = $1`, project)
	})

	pg := Postgres{Pool: pool}
	if pid, err := pg.ProjectForHash(ctx, HashKey(active)); err != nil || pid != project {
		t.Fatalf("active key: %q, %v", pid, err)
	}
	if _, err := pg.ProjectForHash(ctx, HashKey(revoked)); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("revoked key: err = %v, want ErrUnknownKey", err)
	}
	if _, err := pg.ProjectForHash(ctx, HashKey("never-issued")); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key: err = %v, want ErrUnknownKey", err)
	}
	if pid, err := pg.ProjectForHash(ctx, HashKey("tw_dev_localkey")); err != nil || pid != "proj_dev" {
		t.Fatalf("dev seed key: %q, %v", pid, err)
	}
}
