package store

import (
	"context"
	"fmt"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store defines operations needed by the alerter on Postgres.
type Store interface {
	LoadEnabledRules(ctx context.Context) ([]model.AlertRule, error)
	InsertAlertEvent(ctx context.Context, event model.AlertEvent) error
	Ping(ctx context.Context) error
	Close()
}

// PgStore implements Store using pgxpool.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore creates a new Postgres store with connection pool.
func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PgStore{pool: pool}, nil
}

// LoadEnabledRules fetches all enabled alert rules from postgres alert_rules.
func (s *PgStore) LoadEnabledRules(ctx context.Context) ([]model.AlertRule, error) {
	query := `
		SELECT id, project_id, name, metric, threshold, window_seconds, cooldown_seconds, channel, target, enabled, created_at, updated_at
		FROM alert_rules
		WHERE enabled = true
	`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query alert rules: %w", err)
	}
	defer rows.Close()

	var rules []model.AlertRule
	for rows.Next() {
		var r model.AlertRule
		if err := rows.Scan(
			&r.ID,
			&r.ProjectID,
			&r.Name,
			&r.Metric,
			&r.Threshold,
			&r.WindowSeconds,
			&r.CooldownSeconds,
			&r.Channel,
			&r.Target,
			&r.Enabled,
			&r.CreatedAt,
			&r.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan alert rule: %w", err)
		}
		rules = append(rules, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return rules, nil
}

// InsertAlertEvent records an alert fire event in alert_events table.
func (s *PgStore) InsertAlertEvent(ctx context.Context, event model.AlertEvent) error {
	query := `
		INSERT INTO alert_events (rule_id, project_id, value, message, fired_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := s.pool.Exec(
		ctx,
		query,
		event.RuleID,
		event.ProjectID,
		event.Value,
		event.Message,
		event.FiredAt,
	)
	if err != nil {
		return fmt.Errorf("insert alert event: %w", err)
	}
	return nil
}

// Ping checks if Postgres is responding.
func (s *PgStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the underlying pool.
func (s *PgStore) Close() {
	s.pool.Close()
}
