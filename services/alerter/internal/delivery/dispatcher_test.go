package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestChannelDispatcher_Log(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	disp := NewChannelDispatcher(logger, nil)

	rule := model.AlertRule{
		ID:        1,
		ProjectID: "proj_dev",
		Channel:   "log",
	}
	event := model.AlertEvent{
		RuleID:    1,
		ProjectID: "proj_dev",
		Value:     0.75,
		Message:   "test alert",
		FiredAt:   time.Now(),
	}

	err := disp.Deliver(context.Background(), rule, event)
	if err != nil {
		t.Fatalf("unexpected error delivering log alert: %v", err)
	}
}

func TestChannelDispatcher_Webhook(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var received model.WebhookPayload
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", req.Method)
		}
		if req.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json, got %s", req.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(req.Body).Decode(&received)

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`{"status":"ok"}`)),
			Header:     make(http.Header),
		}, nil
	})

	client := &http.Client{Transport: mockTransport}
	disp := NewChannelDispatcher(logger, client)

	rule := model.AlertRule{
		ID:        42,
		Name:      "test webhook rule",
		ProjectID: "proj_webhook",
		Metric:    "error_rate",
		Threshold: 0.5,
		Channel:   "webhook",
		Target:    "http://mock-webhook-target/alert",
	}
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	event := model.AlertEvent{
		RuleID:    42,
		ProjectID: "proj_webhook",
		Value:     0.8,
		Message:   "webhook alert message",
		FiredAt:   now,
	}

	err := disp.Deliver(context.Background(), rule, event)
	if err != nil {
		t.Fatalf("unexpected error delivering webhook alert: %v", err)
	}

	if received.RuleID != 42 || received.ProjectID != "proj_webhook" || received.Value != 0.8 {
		t.Errorf("unexpected payload received by webhook: %+v", received)
	}
}
