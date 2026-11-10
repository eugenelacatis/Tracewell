// Package ingest is the transport-independent part of ingest-svc: API-key auth, the per-project rate
// limit, batch validation, project_id stamping and the Kafka write. The HTTP (internal/api) and gRPC
// (internal/grpcapi) servers decode the request, call Authorize and Write, and map *Error to their own
// status codes (docs/contracts/http.md).
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
)

// MaxBatchBytes is the request size limit from the contract, for both transports.
const MaxBatchBytes = 5 << 20

// RetryAfterSeconds is what a rate-limited client is told to wait (Retry-After header / trailer).
const RetryAfterSeconds = 1

// produceTimeout bounds how long a request waits for the Kafka ack before failing as unavailable.
const produceTimeout = 10 * time.Second

var (
	tracesAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingest_traces_accepted_total",
		Help: "Traces written to traces.raw.",
	})
	spansAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingest_spans_accepted_total",
		Help: "Spans written to traces.raw.",
	})
	rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_rate_limited_total",
		Help: "Requests rejected by the per-project rate limit.",
	}, []string{"project_id"})
	rateLimitErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingest_rate_limit_errors_total",
		Help: "Rate-limit checks that failed (Redis down). Those requests were let through.",
	})
	produceDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "ingest_kafka_produce_duration_seconds",
		Help: "Time from produce to the Kafka ack (acks=all), successful or not.",
	})
)

// Code is what went wrong, independent of transport.
type Code int

const (
	InvalidArgument Code = iota + 1 // HTTP 400, gRPC INVALID_ARGUMENT
	Unauthorized                    // HTTP 401, gRPC UNAUTHENTICATED
	RateLimited                     // HTTP 429 + Retry-After, gRPC RESOURCE_EXHAUSTED + retry-after trailer
	Unavailable                     // HTTP 503, gRPC UNAVAILABLE
	Internal                        // HTTP 500, gRPC UNAVAILABLE (the contract maps every 5xx to it)
)

// Error is returned by Authorize and Write. Msg is safe to send to the client.
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// Keys resolves an API key to its project. auth.Cache in production.
type Keys interface {
	ProjectID(ctx context.Context, key string) (string, error)
}

// Limiter is the per-project rate limit. ratelimit.Redis in production.
type Limiter interface {
	Allow(ctx context.Context, projectID string) (bool, error)
}

// Producer writes one record to traces.raw and returns once the broker has acked it (acks=all).
type Producer interface {
	Produce(ctx context.Context, key string, value []byte) error
}

type Service struct {
	Keys     Keys
	Limiter  Limiter
	Producer Producer
}

// Authorize turns an "Authorization: Bearer <key>" value into a project_id, then applies the project's
// rate limit. Transports call it before reading the body, so rejected requests stay cheap.
func (s *Service) Authorize(ctx context.Context, log *slog.Logger, authorization string) (string, error) {
	key, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || key == "" {
		return "", &Error{Unauthorized, "missing bearer api key"}
	}
	projectID, err := s.Keys.ProjectID(ctx, key)
	if errors.Is(err, auth.ErrUnknownKey) {
		return "", &Error{Unauthorized, "invalid api key"}
	}
	if err != nil {
		log.Error("api key lookup failed", "err", err)
		return "", &Error{Unavailable, "auth backend unavailable"}
	}

	allowed, err := s.Limiter.Allow(ctx, projectID)
	if err != nil {
		// Fail open: a Redis outage must never drop traces.
		rateLimitErrors.Inc()
		log.Warn("rate limit check failed, allowing request", "project_id", projectID, "err", err)
	} else if !allowed {
		rateLimited.WithLabelValues(projectID).Inc()
		return "", &Error{RateLimited, "project over its ingest rate limit"}
	}
	return projectID, nil
}

// Write validates a decoded batch, stamps projectID on it and writes it to Kafka. It returns the
// number of traces accepted. A nil error means the batch is durable in Kafka.
func (s *Service) Write(ctx context.Context, log *slog.Logger, projectID string, batch *tracewellv1.IngestBatch) (int, error) {
	if batch.IdempotencyKey == "" {
		return 0, &Error{InvalidArgument, "idempotencyKey is required"}
	}
	log = log.With("project_id", projectID, "idempotency_key", batch.IdempotencyKey)

	// project_id comes only from the API key. Whatever the client sent is replaced.
	batch.ProjectId = projectID
	spans := 0
	for _, t := range batch.Traces {
		t.ProjectId = projectID
		spans += len(t.Spans)
	}

	value, err := proto.Marshal(batch)
	if err != nil {
		log.Error("marshal batch", "err", err)
		return 0, &Error{Internal, "could not encode batch"}
	}
	ctx, cancel := context.WithTimeout(ctx, produceTimeout)
	defer cancel()
	start := time.Now()
	err = s.Producer.Produce(ctx, projectID, value)
	produceDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		log.Error("kafka produce failed", "err", err)
		return 0, &Error{Unavailable, "could not write to kafka"}
	}

	tracesAccepted.Add(float64(len(batch.Traces)))
	spansAccepted.Add(float64(spans))
	log.Info("accepted batch", "traces", len(batch.Traces), "spans", spans, "batch_seq", batch.BatchSeq)
	return len(batch.Traces), nil
}
