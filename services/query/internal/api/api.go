// Package api is query-svc's HTTP layer: routes, auth middleware, errors and the request log.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/eugenelacatis/tracewell/services/query/internal/auth"
	"github.com/eugenelacatis/tracewell/services/query/internal/model"
	"github.com/eugenelacatis/tracewell/services/query/internal/store"
)

// Store is what the handlers need from ClickHouse.
type Store interface {
	ListTraces(ctx context.Context, p store.ListParams) ([]model.Trace, error)
	GetTrace(ctx context.Context, projectID, traceID string) (model.TraceDetail, error)
}

// Keys resolves an API key to a project_id.
type Keys interface {
	ProjectID(ctx context.Context, key string) (string, error)
}

// Pinger is a dependency /readyz checks.
type Pinger func(ctx context.Context) error

type Server struct {
	Store  Store
	Keys   Keys
	Ready  map[string]Pinger
	Logger *slog.Logger
}

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "query_http_request_duration_seconds",
	Help: "HTTP request latency by route and status.",
}, []string{"route", "status"})

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /v1/traces", s.instrument("/v1/traces", s.requireKey(s.listTraces)))
	mux.Handle("GET /v1/traces/{traceId}", s.instrument("/v1/traces/{traceId}", s.requireKey(s.getTrace)))
	return mux
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for name, ping := range s.Ready {
		if err := ping(ctx); err != nil {
			s.Logger.Warn("not ready", "dependency", name, "err", err)
			http.Error(w, name+" unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = w.Write([]byte("ok"))
}

type ctxKey struct{}

// reqInfo is filled in as a request moves through the middleware, so the request log line can
// include project_id once auth has resolved it.
type reqInfo struct{ projectID string }

func info(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(ctxKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{}
}

func projectFrom(ctx context.Context) string { return info(ctx).projectID }

// requireKey checks "Authorization: Bearer <key>" and puts the key's project_id in the context.
// Handlers only ever read project_id from there, never from the request.
func (s *Server) requireKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing api key")
			return
		}
		pid, err := s.Keys.ProjectID(r.Context(), key)
		if errors.Is(err, auth.ErrUnknownKey) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "invalid api key")
			return
		}
		if err != nil {
			s.Logger.Error("api key lookup failed", "err", err)
			writeError(w, http.StatusServiceUnavailable, "unavailable", "auth backend unavailable")
			return
		}
		info(r.Context()).projectID = pid
		next(w, r)
	}
}

func (s *Server) listTraces(w http.ResponseWriter, r *http.Request) {
	pid := projectFrom(r.Context())
	q := r.URL.Query()

	limit, err := ParseLimit(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	p := store.ListParams{ProjectID: pid, Limit: limit + 1} // one extra row tells us a next page exists
	for name, dst := range map[string]*time.Time{"from": &p.From, "to": &p.To} {
		if v := q.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid_argument", name+" must be RFC 3339")
				return
			}
			*dst = t.UTC()
		}
	}
	if v := q.Get("cursor"); v != "" {
		c, err := DecodeCursor(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "invalid cursor")
			return
		}
		p.AfterStartTime, p.AfterTraceID = c.StartTime, c.TraceID
	}

	traces, err := s.Store.ListTraces(r.Context(), p)
	if err != nil {
		s.Logger.Error("list traces failed", "project_id", pid, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "query failed")
		return
	}
	resp := model.TraceList{Traces: traces}
	if len(traces) > limit {
		resp.Traces = traces[:limit]
		last := resp.Traces[limit-1]
		next := EncodeCursor(Cursor{StartTime: time.Time(last.StartTime), TraceID: last.TraceID})
		resp.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getTrace(w http.ResponseWriter, r *http.Request) {
	pid := projectFrom(r.Context())
	tid := r.PathValue("traceId")
	d, err := s.Store.GetTrace(r.Context(), pid, tid)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "trace not found")
		return
	}
	if err != nil {
		s.Logger.Error("get trace failed", "project_id", pid, "trace_id", tid, "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "query failed")
		return
	}
	writeJSON(w, http.StatusOK, d)
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// instrument logs each request and records its latency for /metrics.
func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		ri := &reqInfo{}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), ctxKey{}, ri)))
		elapsed := time.Since(start)
		requestDuration.WithLabelValues(route, strconv.Itoa(rec.status)).Observe(elapsed.Seconds())
		attrs := []any{"method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", elapsed.Milliseconds()}
		if ri.projectID != "" {
			attrs = append(attrs, "project_id", ri.projectID)
		}
		if tid := r.PathValue("traceId"); tid != "" {
			attrs = append(attrs, "trace_id", tid)
		}
		s.Logger.Info("request", attrs...)
	})
}
