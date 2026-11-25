package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest/ingesttest"
)

func newServer(p *ingesttest.Producer) http.Handler {
	s := &Server{Ingest: ingesttest.NewService(p), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return s.Handler()
}

func post(h http.Handler, key, contentType string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct{ Code, Message string }
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestAcceptsContractExampleAndProducesBinaryProto(t *testing.T) {
	p := &ingesttest.Producer{}
	rec := post(newServer(p), ingesttest.GoodKey, "application/json; charset=utf-8", []byte(ingesttest.ContractExample))

	if rec.Code != http.StatusAccepted || strings.TrimSpace(rec.Body.String()) != `{"accepted":1}` {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	records := p.Records()
	if len(records) != 1 || records[0].Key != "proj_a" {
		t.Fatalf("records = %+v", records)
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(records[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.IdempotencyKey != "9f2c:17" || got.BatchSeq != 17 || got.SdkInstanceId != "9f2c" {
		t.Fatalf("batch fields = %q %d %q", got.IdempotencyKey, got.BatchSeq, got.SdkInstanceId)
	}
	if u := got.Traces[0].Spans[0].Generation.Usage; u.TotalTokens != 12 || u.CostUsd != 0.0000045 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestOverwritesClientProjectID(t *testing.T) {
	batch := &tracewellv1.IngestBatch{
		IdempotencyKey: "k:1",
		ProjectId:      "proj_evil",
		Traces:         []*tracewellv1.Trace{{TraceId: "t1", ProjectId: "proj_evil"}, {TraceId: "t2"}},
	}
	body, _ := proto.Marshal(batch)
	p := &ingesttest.Producer{}
	rec := post(newServer(p), ingesttest.GoodKey, "application/x-protobuf", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(p.Records()[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.ProjectId != "proj_a" || got.Traces[0].ProjectId != "proj_a" || got.Traces[1].ProjectId != "proj_a" {
		t.Fatalf("project ids = %q %q %q", got.ProjectId, got.Traces[0].ProjectId, got.Traces[1].ProjectId)
	}
}

func TestRejections(t *testing.T) {
	tests := []struct {
		name, key, contentType string
		body                   []byte
		status                 int
		code                   string
	}{
		{"missing key", "", "application/json", []byte(ingesttest.ContractExample), 401, "unauthorized"},
		{"bad key", "tw_wrong", "application/json", []byte(ingesttest.ContractExample), 401, "unauthorized"},
		{"rate limited", ingesttest.LimitedKey, "application/json", []byte(ingesttest.ContractExample), 429, "rate_limited"},
		{"postgres down", ingesttest.PGDownKey, "application/json", []byte(ingesttest.ContractExample), 503, "unavailable"},
		{"bad json", ingesttest.GoodKey, "application/json", []byte(`{"traces": [`), 400, "invalid_argument"},
		{"bad protobuf", ingesttest.GoodKey, "application/x-protobuf", []byte{0xff, 0xff, 0xff}, 400, "invalid_argument"},
		{"wrong content type", ingesttest.GoodKey, "text/plain", []byte(ingesttest.ContractExample), 400, "invalid_argument"},
		{"missing idempotency key", ingesttest.GoodKey, "application/json", []byte(`{"traces":[]}`), 400, "invalid_argument"},
		{"too large", ingesttest.GoodKey, "application/json", bytes.Repeat([]byte(" "), ingest.MaxBatchBytes+1), 413, "payload_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ingesttest.Producer{}
			rec := post(newServer(p), tt.key, tt.contentType, tt.body)
			if rec.Code != tt.status || errorCode(t, rec) != tt.code {
				t.Fatalf("got %d %s, want %d %s", rec.Code, rec.Body.String(), tt.status, tt.code)
			}
			if len(p.Records()) != 0 {
				t.Fatalf("produced %d records on a rejected request", len(p.Records()))
			}
		})
	}
}

func TestUnknownJSONFieldsAreIgnored(t *testing.T) {
	p := &ingesttest.Producer{}
	rec := post(newServer(p), ingesttest.GoodKey, "application/json", []byte(`{"idempotencyKey":"k:1","fromTheFuture":1,"traces":[]}`))
	if rec.Code != http.StatusAccepted || len(p.Records()) != 1 {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestKafkaFailureIs503(t *testing.T) {
	p := &ingesttest.Producer{Err: errors.New("broker down")}
	rec := post(newServer(p), ingesttest.GoodKey, "application/json", []byte(ingesttest.ContractExample))
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "unavailable" {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestReadyz(t *testing.T) {
	s := &Server{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Ready: map[string]Pinger{
			"kafka":    func(context.Context) error { return nil },
			"postgres": func(context.Context) error { return errors.New("down") },
		},
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
}

func TestRateLimitedResponseSaysRetryAfterOneSecond(t *testing.T) {
	rec := post(newServer(&ingesttest.Producer{}), ingesttest.LimitedKey, "application/json", []byte(ingesttest.ContractExample))
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d Retry-After=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestRedisDownFailsOpen(t *testing.T) {
	p := &ingesttest.Producer{}
	rec := post(newServer(p), ingesttest.RedisDown, "application/json", []byte(ingesttest.ContractExample))
	if rec.Code != http.StatusAccepted || len(p.Records()) != 1 {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}
