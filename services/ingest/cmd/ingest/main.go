// ingest-svc accepts IngestBatches over HTTP (POST /v1/ingest) and gRPC (IngestService/Ingest),
// stamps the project_id from the API key, applies the per-project rate limit, and writes each batch to
// the Kafka topic traces.raw. A 202 (or gRPC OK) means Kafka has the batch.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eugenelacatis/tracewell/services/ingest/internal/api"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/grpcapi"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ratelimit"
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
	grpcAddr := env("INGEST_GRPC_ADDR", ":9091")
	brokers := strings.Split(env("INGEST_KAFKA_BROKERS", "localhost:9092"), ",")
	pgDSN := env("INGEST_POSTGRES_DSN", "postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable")
	redisAddr := env("INGEST_REDIS_ADDR", "localhost:6379")
	rps, err := strconv.ParseInt(env("INGEST_RATE_LIMIT_RPS", "200"), 10, 64)
	if err != nil || rps < 1 {
		return fmt.Errorf("INGEST_RATE_LIMIT_RPS must be a positive integer, got %q", os.Getenv("INGEST_RATE_LIMIT_RPS"))
	}

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

	// Redis connects lazily, so ingest starts (and fails open) even if Redis is down.
	redisClient := ratelimit.NewClient(redisAddr)
	defer redisClient.Close()

	svc := &ingest.Service{
		Keys:     auth.NewCache(auth.Postgres{Pool: pg}, 60*time.Second),
		Limiter:  ratelimit.Redis{Client: redisClient, Limit: rps},
		Producer: kafkaProducer{kafka},
	}
	srv := &api.Server{
		Ingest: svc,
		Ready: map[string]api.Pinger{
			"kafka":    kafka.Ping,
			"postgres": pg.Ping,
		},
		Logger: logger,
	}
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	grpcSrv := grpcapi.NewServer(svc, logger)
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("ingest-svc listening", "http_addr", addr, "grpc_addr", grpcAddr,
			"kafka_brokers", brokers, "redis_addr", redisAddr, "rate_limit_rps", rps)
		errCh <- httpSrv.ListenAndServe()
	}()
	go func() { errCh <- grpcSrv.Serve(grpcLis) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// SIGTERM: both servers stop accepting work and wait for in-flight requests. Each of those is
	// waiting on its own Kafka ack, so once both return every 202/OK we sent is durable. Flush anyway
	// in case a request was cut off by the timeout, then the deferred Close calls run.
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grpcDone := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(grpcDone) }()
	httpErr := httpSrv.Shutdown(shutdownCtx)
	if errors.Is(httpErr, http.ErrServerClosed) {
		httpErr = nil
	}
	// Wait for gRPC even if HTTP timed out, so no handler is mid-produce when Kafka closes.
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcSrv.Stop() // GracefulStop has no timeout of its own
		<-grpcDone
	}
	return errors.Join(httpErr, kafka.Flush(shutdownCtx))
}
