// Package api serves POST /v1/ingest and the health endpoints, following docs/contracts/http.md.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
)

// MaxBodyBytes is the request size limit from the contract. Bigger bodies get 413.
const MaxBodyBytes = 5 << 20

// produceTimeout bounds how long a request waits for the Kafka ack before answering 503.
const produceTimeout = 10 * time.Second

var (
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ingest_http_request_duration_seconds",
		Help: "POST /v1/ingest latency by response status, including the wait for the Kafka ack.",
	}, []string{"status"})
	tracesAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ingest_traces_accepted_total",
		Help: "Traces written to traces.raw.",
	})
)

// Keys resolves an API key to its project. auth.Cache in production.
type Keys interface {
	ProjectID(ctx context.Context, key string) (string, error)
}

// Producer writes one record to traces.raw and returns once the broker has acked it (acks=all).
type Producer interface {
	Produce(ctx context.Context, key string, value []byte) error
}

// Pinger checks one dependency for /readyz.
type Pinger func(ctx context.Context) error

type Server struct {
	Keys     Keys
	Producer Producer
	Ready    map[string]Pinger
	Logger   *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/ingest", s.ingest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := s.handleIngest(w, r)
	requestDuration.WithLabelValues(strconv.Itoa(status)).Observe(time.Since(start).Seconds())
}

// handleIngest writes the response and returns its status code.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) int {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || key == "" {
		return writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer api key")
	}
	projectID, err := s.Keys.ProjectID(r.Context(), key)
	if errors.Is(err, auth.ErrUnknownKey) {
		return writeError(w, http.StatusUnauthorized, "unauthorized", "invalid api key")
	}
	if err != nil {
		s.Logger.Error("api key lookup failed", "err", err)
		return writeError(w, http.StatusServiceUnavailable, "unavailable", "auth backend unavailable")
	}
	log := s.Logger.With("project_id", projectID)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "body over 5 MB")
	}
	if err != nil {
		return writeError(w, http.StatusBadRequest, "invalid_argument", "could not read body")
	}

	batch := &tracewellv1.IngestBatch{}
	if err := decode(r.Header.Get("Content-Type"), body, batch); err != nil {
		log.Warn("rejected batch", "err", err)
		return writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	}
	if batch.IdempotencyKey == "" {
		return writeError(w, http.StatusBadRequest, "invalid_argument", "idempotencyKey is required")
	}
	log = log.With("idempotency_key", batch.IdempotencyKey)

	// project_id comes only from the API key. Whatever the client put in the body is replaced.
	batch.ProjectId = projectID
	for _, t := range batch.Traces {
		t.ProjectId = projectID
	}

	value, err := proto.Marshal(batch)
	if err != nil {
		log.Error("marshal batch", "err", err)
		return writeError(w, http.StatusInternalServerError, "internal", "could not encode batch")
	}
	ctx, cancel := context.WithTimeout(r.Context(), produceTimeout)
	defer cancel()
	if err := s.Producer.Produce(ctx, projectID, value); err != nil {
		log.Error("kafka produce failed", "err", err)
		return writeError(w, http.StatusServiceUnavailable, "unavailable", "could not write to kafka")
	}

	tracesAccepted.Add(float64(len(batch.Traces)))
	log.Info("accepted batch", "traces", len(batch.Traces), "batch_seq", batch.BatchSeq)
	return writeJSON(w, http.StatusAccepted, map[string]int{"accepted": len(batch.Traces)})
}

// decode parses body as protojson or binary protobuf, picked by Content-Type.
func decode(contentType string, body []byte, batch *tracewellv1.IngestBatch) error {
	mediaType, _, _ := mime.ParseMediaType(contentType)
	switch mediaType {
	case "application/json":
		// Ignore unknown fields so a newer SDK that sends a field this server doesn't know yet
		// still gets its traces stored (binary protobuf behaves the same way by design).
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, batch)
	case "application/x-protobuf":
		return proto.Unmarshal(body, batch)
	default:
		return errors.New("Content-Type must be application/json or application/x-protobuf")
	}
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for name, ping := range s.Ready {
		if err := ping(ctx); err != nil {
			s.Logger.Warn("not ready", "dependency", name, "err", err)
			writeError(w, http.StatusServiceUnavailable, "unavailable", name+" unreachable")
			return
		}
	}
	io.WriteString(w, "ok")
}

func writeJSON(w http.ResponseWriter, status int, v any) int {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
	return status
}

func writeError(w http.ResponseWriter, status int, code, msg string) int {
	type body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	return writeJSON(w, status, map[string]body{"error": {Code: code, Message: msg}})
}
