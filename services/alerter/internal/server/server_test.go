package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	srv := NewServer(":0", nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.handleHealthz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok\n" {
		t.Fatalf("expected body 'ok\n', got %q", rec.Body.String())
	}
}

func TestReadyz_Ready(t *testing.T) {
	readyFunc := func(ctx context.Context) error {
		return nil
	}
	srv := NewServer(":0", readyFunc)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.handleReadyz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ready\n" {
		t.Fatalf("expected body 'ready\n', got %q", rec.Body.String())
	}
}

func TestReadyz_NotReady(t *testing.T) {
	readyFunc := func(ctx context.Context) error {
		return errors.New("db unavailable")
	}
	srv := NewServer(":0", readyFunc)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	srv.handleReadyz(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

