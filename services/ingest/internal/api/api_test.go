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
	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
)

type fakeKeys map[string]string // key -> project; "pgdown" simulates a Postgres failure

func (f fakeKeys) ProjectID(_ context.Context, key string) (string, error) {
	if key == "pgdown" {
		return "", errors.New("connection refused")
	}
	if pid, ok := f[key]; ok {
		return pid, nil
	}
	return "", auth.ErrUnknownKey
}

type record struct {
	key   string
	value []byte
}

type fakeProducer struct {
	records []record
	err     error
}

func (f *fakeProducer) Produce(_ context.Context, key string, value []byte) error {
	if f.err != nil {
		return f.err
	}
	f.records = append(f.records, record{key, value})
	return nil
}

// The example request body from docs/contracts/http.md, so the contract and the server can't drift.
const contractExample = `{
  "idempotencyKey": "9f2c:17",
  "sdkInstanceId": "9f2c",
  "batchSeq": "17",
  "traces": [{
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "sessionId": "chat-42",
    "userId": "user-7",
    "name": "chat turn",
    "tags": ["example"],
    "startTime": "2026-10-26T17:04:05.000000Z",
    "endTime": "2026-10-26T17:04:06.200000Z",
    "spans": [{
      "spanId": "00f067aa0ba902b7",
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "parentSpanId": "",
      "name": "openai.chat",
      "kind": "SPAN_KIND_GENERATION",
      "startTime": "2026-10-26T17:04:05.100000Z",
      "endTime": "2026-10-26T17:04:06.100000Z",
      "status": "STATUS_OK",
      "generation": {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt": "[{\"role\":\"user\",\"content\":\"hi\"}]",
        "completion": "Hello!",
        "params": { "temperature": 0.2 },
        "usage": { "promptTokens": 9, "completionTokens": 3, "totalTokens": 12, "costUsd": 0.0000045 }
      },
      "metadata": {}
    }]
  }]
}`

func newServer(p *fakeProducer) http.Handler {
	s := &Server{
		Keys:     fakeKeys{"tw_good": "proj_a"},
		Producer: p,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
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
	p := &fakeProducer{}
	rec := post(newServer(p), "tw_good", "application/json; charset=utf-8", []byte(contractExample))

	if rec.Code != http.StatusAccepted || strings.TrimSpace(rec.Body.String()) != `{"accepted":1}` {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	if len(p.records) != 1 || p.records[0].key != "proj_a" {
		t.Fatalf("records = %+v", p.records)
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(p.records[0].value, &got); err != nil {
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
	p := &fakeProducer{}
	rec := post(newServer(p), "tw_good", "application/x-protobuf", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(p.records[0].value, &got); err != nil {
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
		{"missing key", "", "application/json", []byte(contractExample), 401, "unauthorized"},
		{"bad key", "tw_wrong", "application/json", []byte(contractExample), 401, "unauthorized"},
		{"postgres down", "pgdown", "application/json", []byte(contractExample), 503, "unavailable"},
		{"bad json", "tw_good", "application/json", []byte(`{"traces": [`), 400, "invalid_argument"},
		{"unknown json field", "tw_good", "application/json", []byte(`{"idempotencyKey":"k","nope":1}`), 400, "invalid_argument"},
		{"bad protobuf", "tw_good", "application/x-protobuf", []byte{0xff, 0xff, 0xff}, 400, "invalid_argument"},
		{"wrong content type", "tw_good", "text/plain", []byte(contractExample), 400, "invalid_argument"},
		{"missing idempotency key", "tw_good", "application/json", []byte(`{"traces":[]}`), 400, "invalid_argument"},
		{"too large", "tw_good", "application/json", bytes.Repeat([]byte(" "), MaxBodyBytes+1), 413, "payload_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProducer{}
			rec := post(newServer(p), tt.key, tt.contentType, tt.body)
			if rec.Code != tt.status || errorCode(t, rec) != tt.code {
				t.Fatalf("got %d %s, want %d %s", rec.Code, rec.Body.String(), tt.status, tt.code)
			}
			if len(p.records) != 0 {
				t.Fatalf("produced %d records on a rejected request", len(p.records))
			}
		})
	}
}

func TestKafkaFailureIs503(t *testing.T) {
	p := &fakeProducer{err: errors.New("broker down")}
	rec := post(newServer(p), "tw_good", "application/json", []byte(contractExample))
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
