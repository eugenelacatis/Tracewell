// query-svc serves the read API the dashboard uses: GET /v1/traces and GET /v1/traces/{traceId}.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eugenelacatis/tracewell/services/query/internal/api"
	"github.com/eugenelacatis/tracewell/services/query/internal/auth"
	"github.com/eugenelacatis/tracewell/services/query/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("query-svc exited", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(logger *slog.Logger) error {
	addr := env("QUERY_HTTP_ADDR", ":8082")
	chDSN := env("QUERY_CLICKHOUSE_DSN", "clickhouse://tracewell:tracewell@localhost:9000/tracewell")
	pgDSN := env("QUERY_POSTGRES_DSN", "postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	ch, err := store.OpenClickHouse(chDSN)
	if err != nil {
		return err
	}
	defer ch.Close()

	pg, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()

	srv := &api.Server{
		Store: ch,
		Keys:  auth.NewCache(auth.Postgres{Pool: pg}, 60*time.Second),
		Ready: map[string]api.Pinger{
			"clickhouse": ch.Ping,
			"postgres":   pg.Ping,
		},
		Logger: logger,
	}
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("query-svc listening", "addr", addr)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// SIGTERM: stop accepting connections, let in-flight requests finish, then close the DB pools
	// (the deferred Close calls). query-svc has no producers to flush.
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
