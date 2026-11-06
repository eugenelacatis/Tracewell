package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
)

type mockStore struct {
	mu           sync.Mutex
	rules        []model.AlertRule
	events       []model.AlertEvent
	loadErr      error
	insertErr    error
	pingErr      error
	closedCalled bool
}

func (m *mockStore) LoadEnabledRules(ctx context.Context) ([]model.AlertRule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return m.rules, nil
}

func (m *mockStore) InsertAlertEvent(ctx context.Context, event model.AlertEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertErr != nil {
		return m.insertErr
	}
	m.events = append(m.events, event)
	return nil
}

func (m *mockStore) Ping(ctx context.Context) error {
	return m.pingErr
}

func (m *mockStore) Close() {
	m.closedCalled = true
}

func (m *mockStore) getEvents() []model.AlertEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]model.AlertEvent, len(m.events))
	copy(res, m.events)
	return res
}

type mockDispatcher struct {
	mu        sync.Mutex
	delivered []model.AlertEvent
}

func (m *mockDispatcher) Deliver(ctx context.Context, rule model.AlertRule, event model.AlertEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = append(m.delivered, event)
	return nil
}

func (m *mockDispatcher) getDelivered() []model.AlertEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]model.AlertEvent, len(m.delivered))
	copy(res, m.delivered)
	return res
}

func TestWindowMath(t *testing.T) {
	tracker := NewWindowTracker()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

	// Initially empty
	rate, total, errors := tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 0 || errors != 0 || rate != 0.0 {
		t.Fatalf("expected 0 for empty tracker, got total=%d, errors=%d, rate=%f", total, errors, rate)
	}

	// Add 10 spans at now (4 errors, 6 ok)
	tracker.AddSpans("proj_1", 10, 4, now)

	rate, total, errors = tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 10 {
		t.Errorf("expected total 10, got %d", total)
	}
	if errors != 4 {
		t.Errorf("expected errors 4, got %d", errors)
	}
	if rate != 0.40 {
		t.Errorf("expected rate 0.40, got %f", rate)
	}

	// Add older spans (outside 60s window: 90 seconds ago)
	tracker.AddSpans("proj_1", 20, 20, now.Add(-90*time.Second))

	// Rate within 60s window should still be 4/10 = 0.40
	rate, total, errors = tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 10 || errors != 4 || rate != 0.40 {
		t.Errorf("expected 60s window to ignore spans from 90s ago; got total=%d, errors=%d, rate=%f", total, errors, rate)
	}

	// Rate within 120s window includes older spans: (4 + 20) / (10 + 20) = 24 / 30 = 0.80
	rate, total, errors = tracker.CalculateErrorRate("proj_1", now, 120)
	if total != 30 || errors != 24 || rate != 0.80 {
		t.Errorf("expected 120s window to include older spans; got total=%d, errors=%d, rate=%f", total, errors, rate)
	}

	// Test PruneOlderThan
	tracker.PruneOlderThan(now.Add(-60 * time.Second))
	rate, total, _ = tracker.CalculateErrorRate("proj_1", now, 120)
	if total != 10 {
		t.Errorf("expected total 10 after pruning records older than 60s, got %d", total)
	}
}

func TestThresholdAndCooldown(t *testing.T) {
	ctx := context.Background()
	store := &mockStore{}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	currentTime := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		return currentTime
	}

	engine := NewAlertEngine(store, dispatcher, logger, clock)

	rule := model.AlertRule{
		ID:              101,
		ProjectID:       "proj_dev",
		Name:            "error rate above 50%",
		Metric:          "error_rate",
		Threshold:       0.5,
		WindowSeconds:   60,
		CooldownSeconds: 60,
		Channel:         "log",
		Enabled:         true,
	}
	engine.SetRules([]model.AlertRule{rule})

	// 1. Send burst with 80% error rate at t=0s -> should FIRE
	batch := &tracewellv1.IngestBatch{
		ProjectId: "proj_dev",
		Traces: []*tracewellv1.Trace{
			{
				TraceId:   "trace1",
				ProjectId: "proj_dev",
				Spans: []*tracewellv1.Span{
					{SpanId: "s1", Status: tracewellv1.Status_STATUS_ERROR},
					{SpanId: "s2", Status: tracewellv1.Status_STATUS_ERROR},
					{SpanId: "s3", Status: tracewellv1.Status_STATUS_ERROR},
					{SpanId: "s4", Status: tracewellv1.Status_STATUS_ERROR},
					{SpanId: "s5", Status: tracewellv1.Status_STATUS_OK},
				},
			},
		},
	}

	if err := engine.ProcessBatch(ctx, batch); err != nil {
		t.Fatalf("unexpected error processing batch: %v", err)
	}

	events := store.getEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 alert event after first burst, got %d", len(events))
	}
	if events[0].RuleID != 101 || events[0].ProjectID != "proj_dev" || events[0].Value != 0.8 {
		t.Errorf("unexpected event content: %+v", events[0])
	}
	if len(dispatcher.getDelivered()) != 1 {
		t.Errorf("expected 1 delivered alert, got %d", len(dispatcher.getDelivered()))
	}

	// 2. Send another burst at t=15s (inside 60s cooldown) -> should NOT fire
	currentTime = currentTime.Add(15 * time.Second)
	if err := engine.ProcessBatch(ctx, batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(store.getEvents()) != 1 {
		t.Fatalf("expected still 1 alert event due to cooldown, got %d", len(store.getEvents()))
	}

	// 3. Send another burst at t=70s (cooldown expired: 70s > 60s) -> should FIRE AGAIN
	currentTime = currentTime.Add(55 * time.Second) // total t=70s from first fire
	if err := engine.ProcessBatch(ctx, batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events = store.getEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 alert events after cooldown expired, got %d", len(events))
	}
}

func TestRuleReload(t *testing.T) {
	ctx := context.Background()
	store := &mockStore{
		rules: []model.AlertRule{
			{
				ID:        1,
				ProjectID: "proj_dev",
				Name:      "rule 1",
				Metric:    "error_rate",
				Enabled:   true,
			},
			{
				ID:        2,
				ProjectID: "proj_dev",
				Name:      "rule 2 (disabled)",
				Metric:    "error_rate",
				Enabled:   false,
			},
		},
	}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewAlertEngine(store, dispatcher, logger, nil)

	// Reload rules
	if err := engine.ReloadRules(ctx); err != nil {
		t.Fatalf("failed reloading rules: %v", err)
	}

	activeRules := engine.GetRules()
	if len(activeRules) != 1 {
		t.Fatalf("expected 1 active rule (ignoring disabled), got %d", len(activeRules))
	}
	if activeRules[0].ID != 1 {
		t.Errorf("expected active rule ID 1, got %d", activeRules[0].ID)
	}

	// Update store rules (add rule 3, remove rule 1)
	store.mu.Lock()
	store.rules = []model.AlertRule{
		{
			ID:        3,
			ProjectID: "proj_other",
			Name:      "rule 3",
			Metric:    "error_rate",
			Enabled:   true,
		},
	}
	store.mu.Unlock()

	if err := engine.ReloadRules(ctx); err != nil {
		t.Fatalf("failed reloading rules second time: %v", err)
	}

	activeRules = engine.GetRules()
	if len(activeRules) != 1 || activeRules[0].ID != 3 {
		t.Fatalf("expected active rule ID 3 after reload, got %+v", activeRules)
	}
}

func TestEventInsertFailureIsReturnedAndDoesNotStartCooldown(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	store := &mockStore{
		insertErr: errors.New("postgres unavailable"),
	}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng := NewAlertEngine(store, dispatcher, logger, func() time.Time { return now })
	eng.SetRules([]model.AlertRule{{
		ID: 1, ProjectID: "proj_dev", Name: "errors", Metric: "error_rate",
		Threshold: 0.5, WindowSeconds: 60, CooldownSeconds: 300, Enabled: true,
	}})
	batch := &tracewellv1.IngestBatch{ProjectId: "proj_dev", Traces: []*tracewellv1.Trace{{
		Spans: []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
	}}}

	if err := eng.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected alert event insert failure to propagate")
	}
	store.mu.Lock()
	store.insertErr = nil
	store.mu.Unlock()
	now = now.Add(time.Second)
	if err := eng.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("expected redelivery to persist the alert event: %v", err)
	}
	if got := len(store.getEvents()); got != 1 {
		t.Fatalf("expected one persisted alert event after retry, got %d", got)
	}
}
