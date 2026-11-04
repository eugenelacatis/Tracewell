// ingest-svc accepts IngestBatches over HTTP (POST /v1/ingest), stamps the project_id from the API
// key, and writes each batch to the Kafka topic traces.raw. A 202 means Kafka has the batch.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eugenelacatis/tracewell/services/ingest/internal/api"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
)

const (
	topic          = "traces.raw"
	maxRecordBytes = 6 << 20
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("ingest-svc exited", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// kafkaProducer sends to traces.raw and waits for the ack.
type kafkaProducer struct{ client *kgo.Client }

func (p kafkaProducer) Produce(ctx context.Context, key string, value []byte) error {
	return p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr()
}

func run(logger *slog.Logger) error {
	addr := env("INGEST_HTTP_ADDR", ":8081")
	brokers := strings.Split(env("INGEST_KAFKA_BROKERS", "localhost:9092"), ",")
	pgDSN := env("INGEST_POSTGRES_DSN", "postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	pg, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pg.Close()

	// acks=all: the leader answers only after every in-sync replica has the record. franz-go's
	// producer is idempotent by default, so its own internal retries can't write a record twice.
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// franz-go refuses records over 1 MB by default. A 5 MB request body (the contract limit)
		// stays under 6 MiB as binary protobuf. The topic's max.message.bytes must allow it too.
		kgo.ProducerBatchMaxBytes(maxRecordBytes),
	)
	if err != nil {
		return err
	}
	defer kafka.Close()

	srv := &api.Server{
		Keys:     auth.NewCache(auth.Postgres{Pool: pg}, 60*time.Second),
		Producer: kafkaProducer{kafka},
		Ready: map[string]api.Pinger{
			"kafka":    kafka.Ping,
			"postgres": pg.Ping,
		},
		Logger: logger,
	}
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("ingest-svc listening", "addr", addr, "kafka_brokers", brokers)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// SIGTERM: stop accepting connections and wait for in-flight requests. Each of those is waiting
	// on its own Kafka ack, so when Shutdown returns every 202 we sent is durable. Flush anyway in
	// case a request was cut off by the timeout, then the deferred Close calls run.
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return kafka.Flush(shutdownCtx)
}
