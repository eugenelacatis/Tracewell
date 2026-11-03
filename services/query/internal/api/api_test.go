package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eugenelacatis/tracewell/services/query/internal/auth"
	"github.com/eugenelacatis/tracewell/services/query/internal/model"
	"github.com/eugenelacatis/tracewell/services/query/internal/store"
)

type fakeKeys map[string]string

func (f fakeKeys) ProjectID(_ context.Context, key string) (string, error) {
	if pid, ok := f[key]; ok {
		return pid, nil
	}
	return "", auth.ErrUnknownKey
}

type fakeStore struct {
	traces   []model.Trace // newest first
	lastList store.ListParams
}

func (f *fakeStore) ListTraces(_ context.Context, p store.ListParams) ([]model.Trace, error) {
	f.lastList = p
	out := []model.Trace{}
	for _, t := range f.traces {
		if p.AfterTraceID != "" {
			st := time.Time(t.StartTime)
			if !(st.Before(p.AfterStartTime) || (st.Equal(p.AfterStartTime) && t.TraceID < p.AfterTraceID)) {
				continue
			}
		}
		if len(out) == p.Limit {
			break
		}
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeStore) GetTrace(_ context.Context, projectID, traceID string) (model.TraceDetail, error) {
	for _, t := range f.traces {
		if projectID == "proj_a" && t.TraceID == traceID {
			return model.TraceDetail{Trace: t, Spans: []model.Span{}, Evals: []model.Eval{}}, nil
		}
	}
	return model.TraceDetail{}, store.ErrNotFound
}

func newTestServer(fs *fakeStore) http.Handler {
	s := &Server{
		Store:  fs,
		Keys:   fakeKeys{"good": "proj_a", "other": "proj_b"},
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	return s.Handler()
}

func do(t *testing.T, h http.Handler, key, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

func errCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func traceAt(id string, sec int) model.Trace {
	return model.Trace{TraceID: id, Tags: []string{}, StartTime: model.Time(time.Unix(int64(sec), 0).UTC())}
}

func TestUnauthorized(t *testing.T) {
	h := newTestServer(&fakeStore{})
	for _, key := range []string{"", "wrong"} {
		rec, body := do(t, h, key, "/v1/traces")
		if rec.Code != http.StatusUnauthorized || errCode(body) != "unauthorized" {
			t.Errorf("key %q: got %d %v", key, rec.Code, body)
		}
	}
}

func TestNotFoundAndProjectScoping(t *testing.T) {
	h := newTestServer(&fakeStore{traces: []model.Trace{traceAt("t1", 1)}})
	if rec, _ := do(t, h, "good", "/v1/traces/t1"); rec.Code != http.StatusOK {
		t.Fatalf("own trace: got %d", rec.Code)
	}
	for _, c := range []struct{ key, id string }{{"good", "missing"}, {"other", "t1"}} {
		rec, body := do(t, h, c.key, "/v1/traces/"+c.id)
		if rec.Code != http.StatusNotFound || errCode(body) != "not_found" {
			t.Errorf("%+v: got %d %v", c, rec.Code, body)
		}
	}
}

func TestPaging(t *testing.T) {
	fs := &fakeStore{traces: []model.Trace{traceAt("c", 3), traceAt("b", 2), traceAt("a", 1)}}
	h := newTestServer(fs)

	rec, body := do(t, h, "good", "/v1/traces?limit=2")
	if rec.Code != 200 {
		t.Fatalf("got %d", rec.Code)
	}
	if fs.lastList.ProjectID != "proj_a" {
		t.Fatalf("project_id should come from the key, got %q", fs.lastList.ProjectID)
	}
	if n := len(body["traces"].([]any)); n != 2 {
		t.Fatalf("want 2 traces, got %d", n)
	}
	next, ok := body["nextCursor"].(string)
	if !ok {
		t.Fatalf("want a nextCursor, got %v", body["nextCursor"])
	}

	_, body = do(t, h, "good", "/v1/traces?limit=2&cursor="+next)
	traces := body["traces"].([]any)
	if len(traces) != 1 || traces[0].(map[string]any)["traceId"] != "a" {
		t.Fatalf("second page wrong: %v", traces)
	}
	if body["nextCursor"] != nil {
		t.Fatalf("last page should have nextCursor null, got %v", body["nextCursor"])
	}
}

func TestBadParams(t *testing.T) {
	h := newTestServer(&fakeStore{})
	for _, q := range []string{"limit=abc", "cursor=%25%25", "from=yesterday"} {
		rec, body := do(t, h, "good", "/v1/traces?"+q)
		if rec.Code != http.StatusBadRequest || errCode(body) != "invalid_argument" {
			t.Errorf("%s: got %d %v", q, rec.Code, body)
		}
	}
}

func TestEmptyListIsArray(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/traces", nil)
	req.Header.Set("Authorization", "Bearer good")
	newTestServer(&fakeStore{}).ServeHTTP(rec, req)
	if got := rec.Body.String(); got != "{\"traces\":[],\"nextCursor\":null}\n" {
		t.Fatalf("got %s", got)
	}
}
