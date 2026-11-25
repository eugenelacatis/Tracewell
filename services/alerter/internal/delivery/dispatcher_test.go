package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/metrics"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
	dto "github.com/prometheus/client_model/go"
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
	disp.SetBackoff(nil)

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

func TestChannelDispatcher_Slack(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var received model.SlackPayload
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", req.Method)
		}
		if req.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json, got %s", req.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(req.Body).Decode(&received); err != nil {
			t.Errorf("failed to decode Slack payload: %v", err)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`ok`)),
			Header:     make(http.Header),
		}, nil
	})

	client := &http.Client{Transport: mockTransport}
	disp := NewChannelDispatcher(logger, client)
	disp.SetBackoff(nil)

	firedAt := time.Date(2026, 11, 4, 12, 5, 0, 0, time.UTC)
	rule := model.AlertRule{
		ID:            55,
		Name:          "cost over threshold",
		ProjectID:     "proj_slack",
		Metric:        "cost_usd",
		Threshold:     1.50,
		WindowSeconds: 300,
		Channel:       "slack",
		Target:        "https://hooks.slack.com/services/test/hook",
	}
	event := model.AlertEvent{
		RuleID:    55,
		ProjectID: "proj_slack",
		Value:     2.15,
		Message:   "cost_usd 2.15 > 1.50 over 300s",
		FiredAt:   firedAt,
	}

	err := disp.Deliver(context.Background(), rule, event)
	if err != nil {
		t.Fatalf("unexpected error delivering slack alert: %v", err)
	}

	// Verify required components in Slack message:
	// rule name, project, value, threshold, window, and URL http://localhost:3000/traces?from=...&to=...
	text := received.Text
	if !strings.Contains(text, "cost over threshold") {
		t.Errorf("Slack message missing rule name: %s", text)
	}
	if !strings.Contains(text, "proj_slack") {
		t.Errorf("Slack message missing project ID: %s", text)
	}
	if !strings.Contains(text, "2.15") {
		t.Errorf("Slack message missing value: %s", text)
	}
	if !strings.Contains(text, "1.50") {
		t.Errorf("Slack message missing threshold: %s", text)
	}
	if !strings.Contains(text, "300") {
		t.Errorf("Slack message missing window: %s", text)
	}

	// Verify trace URL
	fromExpected := url.QueryEscape(firedAt.Add(-300 * time.Second).UTC().Format(time.RFC3339))
	toExpected := url.QueryEscape(firedAt.UTC().Format(time.RFC3339))
	expectedLink := fmt.Sprintf("http://localhost:3000/traces?from=%s&to=%s", fromExpected, toExpected)
	if !strings.Contains(text, expectedLink) {
		t.Errorf("Slack message missing or invalid trace URL.\nExpected: %s\nGot: %s", expectedLink, text)
	}
}

func TestChannelDispatcher_RetryWithBackoff(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var attempts int32
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&attempts, 1)
		if n <= 2 {
			// Fail first 2 attempts
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewBufferString(`server error`)),
				Header:     make(http.Header),
			}, nil
		}
		// Succeed on 3rd attempt
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`ok`)),
			Header:     make(http.Header),
		}, nil
	})

	client := &http.Client{Transport: mockTransport}
	disp := NewChannelDispatcher(logger, client)

	var slept []time.Duration
	disp.SetSleep(func(d time.Duration) {
		slept = append(slept, d)
	})
	disp.SetBackoff([]time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond})

	rule := model.AlertRule{
		ID:        99,
		Name:      "retry rule",
		ProjectID: "proj_retry",
		Metric:    "error_rate",
		Threshold: 0.1,
		Channel:   "webhook",
		Target:    "http://mock-webhook/retry",
	}
	event := model.AlertEvent{
		RuleID:    99,
		ProjectID: "proj_retry",
		Value:     0.2,
		FiredAt:   time.Now(),
	}

	err := disp.Deliver(context.Background(), rule, event)
	if err != nil {
		t.Fatalf("expected delivery to succeed on 3rd attempt, got error: %v", err)
	}

	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", atomic.LoadInt32(&attempts))
	}
	if len(slept) != 2 {
		t.Errorf("expected 2 backoff sleeps before 3rd attempt, got %d", len(slept))
	}
}

func TestChannelDispatcher_RetryExhaustionIncrementsMetric(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var attempts int32
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&attempts, 1)
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(bytes.NewBufferString(`bad gateway`)),
			Header:     make(http.Header),
		}, nil
	})

	client := &http.Client{Transport: mockTransport}
	disp := NewChannelDispatcher(logger, client)

	disp.SetSleep(func(d time.Duration) {})
	disp.SetBackoff([]time.Duration{time.Millisecond, time.Millisecond, time.Millisecond})

	rule := model.AlertRule{
		ID:        100,
		Name:      "failing rule",
		ProjectID: "proj_fail",
		Metric:    "error_rate",
		Threshold: 0.1,
		Channel:   "slack",
		Target:    "https://hooks.slack.com/services/fail",
	}
	event := model.AlertEvent{
		RuleID:    100,
		ProjectID: "proj_fail",
		Value:     0.5,
		FiredAt:   time.Now(),
	}

	var mBefore dto.Metric
	_ = metrics.DeliveryErrors.WithLabelValues("slack").Write(&mBefore)
	countBefore := mBefore.GetCounter().GetValue()

	err := disp.Deliver(context.Background(), rule, event)
	if err == nil {
		t.Fatalf("expected error after retries exhausted, got nil")
	}

	// 1 initial + 3 retries = 4 attempts total
	if atomic.LoadInt32(&attempts) != 4 {
		t.Errorf("expected 4 attempts total, got %d", atomic.LoadInt32(&attempts))
	}

	var mAfter dto.Metric
	_ = metrics.DeliveryErrors.WithLabelValues("slack").Write(&mAfter)
	countAfter := mAfter.GetCounter().GetValue()

	if countAfter-countBefore != 1 {
		t.Errorf("expected alerter_delivery_errors_total to increment by 1, before=%f, after=%f", countBefore, countAfter)
	}
}
