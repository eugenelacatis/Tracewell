// Package api serves POST /v1/ingest and the health endpoints, following docs/contracts/http.md.
// Auth, rate limiting, validation and the Kafka write live in internal/ingest, shared with gRPC.
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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
)

const ingestRoute = "POST /v1/ingest"

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "ingest_http_request_duration_seconds",
	Help: "HTTP request latency by route and response status, including the wait for the Kafka ack.",
}, []string{"route", "status"})

// Pinger checks one dependency for /readyz.
type Pinger func(ctx context.Context) error

type Server struct {
	Ingest *ingest.Service
	// Ready lists what /readyz checks: Kafka and Postgres. Not Redis, because the rate limit fails
	// open, so a Redis outage must not take ingest out of rotation.
	Ready  map[string]Pinger
	Logger *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(ingestRoute, s.ingest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := s.handleIngest(w, r)
	requestDuration.WithLabelValues(ingestRoute, strconv.Itoa(status)).Observe(time.Since(start).Seconds())
}

// handleIngest writes the response and returns its status code.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) int {
	log := s.Logger
	if id := r.Header.Get("X-Request-Id"); id != "" {
		log = log.With("request_id", id)
	}
	projectID, err := s.Ingest.Authorize(r.Context(), log, r.Header.Get("Authorization"))
	if err != nil {
		return writeIngestError(w, err)
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, ingest.MaxBatchBytes))
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "body over 5 MB")
	}
	if err != nil {
		return writeError(w, http.StatusBadRequest, "invalid_argument", "could not read body")
	}
	batch := &tracewellv1.IngestBatch{}
	if err := decode(r.Header.Get("Content-Type"), body, batch); err != nil {
		log.Warn("rejected batch", "project_id", projectID, "err", err)
		return writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	}

	accepted, err := s.Ingest.Write(r.Context(), log, projectID, batch)
	if err != nil {
		return writeIngestError(w, err)
	}
	return writeJSON(w, http.StatusAccepted, map[string]int{"accepted": accepted})
}

// writeIngestError maps an *ingest.Error to the HTTP status and error code in the contract.
func writeIngestError(w http.ResponseWriter, err error) int {
	var e *ingest.Error
	if !errors.As(err, &e) {
		return writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
	switch e.Code {
	case ingest.InvalidArgument:
		return writeError(w, http.StatusBadRequest, "invalid_argument", e.Msg)
	case ingest.Unauthorized:
		return writeError(w, http.StatusUnauthorized, "unauthorized", e.Msg)
	case ingest.RateLimited:
		w.Header().Set("Retry-After", strconv.Itoa(ingest.RetryAfterSeconds))
		return writeError(w, http.StatusTooManyRequests, "rate_limited", e.Msg)
	case ingest.Unavailable:
		return writeError(w, http.StatusServiceUnavailable, "unavailable", e.Msg)
	default:
		return writeError(w, http.StatusInternalServerError, "internal", e.Msg)
	}
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
