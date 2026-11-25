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
	"github.com/eugenelacatis/tracewell/services/alerter/internal/metrics"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	mu          sync.Mutex
	delivered   []model.AlertEvent
	deliveryErr error
}

func (m *mockDispatcher) Deliver(ctx context.Context, rule model.AlertRule, event model.AlertEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deliveryErr != nil {
		return m.deliveryErr
	}
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

	// 1. Initially empty
	rate, total, errors, valid := tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 0 || errors != 0 || rate != 0.0 || valid {
		t.Fatalf("expected 0/false for empty tracker, got total=%d, errors=%d, rate=%f, valid=%v", total, errors, rate, valid)
	}

	// 2. Add 10 spans at now (4 errors, 6 ok)
	tracker.AddSpans("proj_1", 10, 4, now)

	rate, total, errors, valid = tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 10 {
		t.Errorf("expected total 10, got %d", total)
	}
	if errors != 4 {
		t.Errorf("expected errors 4, got %d", errors)
	}
	if rate != 0.40 {
		t.Errorf("expected rate 0.40, got %f", rate)
	}
	if !valid {
		t.Errorf("expected valid=true when total >= 5")
	}

	// 3. Add older spans (outside 60s window: 90 seconds ago)
	tracker.AddSpans("proj_1", 20, 20, now.Add(-90*time.Second))

	// Rate within 60s window should still be 4/10 = 0.40
	rate, total, errors, valid = tracker.CalculateErrorRate("proj_1", now, 60)
	if total != 10 || errors != 4 || rate != 0.40 {
		t.Errorf("expected 60s window to ignore spans from 90s ago; got total=%d, errors=%d, rate=%f", total, errors, rate)
	}

	// Rate within 120s window includes older spans: (4 + 20) / (10 + 20) = 24 / 30 = 0.80
	rate, total, errors, valid = tracker.CalculateErrorRate("proj_1", now, 120)
	if total != 30 || errors != 24 || rate != 0.80 {
		t.Errorf("expected 120s window to include older spans; got total=%d, errors=%d, rate=%f", total, errors, rate)
	}

	// 4. Test CostUSD window math
	tracker.AddSpan("proj_1", false, 0.50, now.Add(-10*time.Second))
	tracker.AddSpan("proj_1", false, 0.75, now.Add(-20*time.Second))
	tracker.AddSpan("proj_1", false, 1.00, now.Add(-100*time.Second)) // outside 60s window

	cost60 := tracker.CalculateCostUSD("proj_1", now, 60)
	if cost60 != 1.25 {
		t.Errorf("expected 60s cost 1.25, got %f", cost60)
	}
	cost120 := tracker.CalculateCostUSD("proj_1", now, 120)
	if cost120 != 2.25 {
		t.Errorf("expected 120s cost 2.25, got %f", cost120)
	}

	// 5. Test P95Latency window math
	// Add 5 traces with durations: 100, 200, 300, 400, 500ms
	tracker.AddTrace("proj_1", 100.0, now.Add(-5*time.Second))
	tracker.AddTrace("proj_1", 200.0, now.Add(-10*time.Second))
	tracker.AddTrace("proj_1", 300.0, now.Add(-15*time.Second))
	tracker.AddTrace("proj_1", 400.0, now.Add(-20*time.Second))
	tracker.AddTrace("proj_1", 500.0, now.Add(-25*time.Second))

	p95, count, p95Valid := tracker.CalculateP95Latency("proj_1", now, 60)
	if !p95Valid {
		t.Fatalf("expected p95 to be valid with 5 traces")
	}
	if count != 5 {
		t.Errorf("expected count 5, got %d", count)
	}
	// For 5 traces, ceil(0.95*5)-1 = 4 -> index 4 is 500.0
	if p95 != 500.0 {
		t.Errorf("expected p95 of [100,200,300,400,500] to be 500.0, got %f", p95)
	}

	// 6. Test PruneOlderThan
	tracker.PruneOlderThan(now.Add(-60 * time.Second))
	_, total, _, _ = tracker.CalculateErrorRate("proj_1", now, 120)
	if total != 12 { // 10 original spans + 2 within 60s cost spans
		t.Errorf("expected total 12 after pruning records older than 60s, got %d", total)
	}
}

func TestMinSampleGuards(t *testing.T) {
	ctx := context.Background()
	store := &mockStore{}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	engine := NewAlertEngine(store, dispatcher, logger, clock)

	rules := []model.AlertRule{
		{
			ID:              1,
			ProjectID:       "proj_guard",
			Name:            "error rate guard",
			Metric:          "error_rate",
			Threshold:       0.1,
			WindowSeconds:   60,
			CooldownSeconds: 60,
			Channel:         "log",
			Enabled:         true,
		},
		{
			ID:              2,
			ProjectID:       "proj_guard",
			Name:            "p95 latency guard",
			Metric:          "p95_latency_ms",
			Threshold:       500.0,
			WindowSeconds:   60,
			CooldownSeconds: 60,
			Channel:         "log",
			Enabled:         true,
		},
		{
			ID:              3,
			ProjectID:       "proj_guard",
			Name:            "cost usd rule",
			Metric:          "cost_usd",
			Threshold:       1.0,
			WindowSeconds:   60,
			CooldownSeconds: 60,
			Channel:         "log",
			Enabled:         true,
		},
	}
	engine.SetRules(rules)

	// A. Send 4 error spans (100% error rate) and 4 slow traces (duration 1000ms)
	// Because samples < 5, neither error_rate nor p95_latency_ms should fire!
	batch4 := &tracewellv1.IngestBatch{
		ProjectId: "proj_guard",
		Traces: []*tracewellv1.Trace{
			{
				TraceId:   "t1",
				StartTime: timestamppb.New(now.Add(-10 * time.Second)),
				EndTime:   timestamppb.New(now.Add(-9 * time.Second)), // 1000ms duration
				Spans:     []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
			},
			{
				TraceId:   "t2",
				StartTime: timestamppb.New(now.Add(-10 * time.Second)),
				EndTime:   timestamppb.New(now.Add(-9 * time.Second)), // 1000ms duration
				Spans:     []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
			},
			{
				TraceId:   "t3",
				StartTime: timestamppb.New(now.Add(-10 * time.Second)),
				EndTime:   timestamppb.New(now.Add(-9 * time.Second)), // 1000ms duration
				Spans:     []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
			},
			{
				TraceId:   "t4",
				StartTime: timestamppb.New(now.Add(-10 * time.Second)),
				EndTime:   timestamppb.New(now.Add(-9 * time.Second)), // 1000ms duration
				Spans:     []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
			},
		},
	}

	if err := engine.ProcessBatch(ctx, batch4); err != nil {
		t.Fatalf("unexpected error processing 4 traces: %v", err)
	}

	if len(store.getEvents()) != 0 {
		t.Fatalf("expected 0 alert events with < 5 samples, got %d", len(store.getEvents()))
	}

	// B. Now send the 5th trace & span: should trigger BOTH error_rate and p95_latency_ms!
	batch5th := &tracewellv1.IngestBatch{
		ProjectId: "proj_guard",
		Traces: []*tracewellv1.Trace{
			{
				TraceId:   "t5",
				StartTime: timestamppb.New(now.Add(-5 * time.Second)),
				EndTime:   timestamppb.New(now.Add(-4 * time.Second)), // 1000ms
				Spans:     []*tracewellv1.Span{{Status: tracewellv1.Status_STATUS_ERROR}},
			},
		},
	}

	if err := engine.ProcessBatch(ctx, batch5th); err != nil {
		t.Fatalf("unexpected error processing 5th trace: %v", err)
	}

	events := store.getEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 alert events (error_rate and p95_latency_ms) after 5th sample, got %d", len(events))
	}

	// C. Test cost_usd: 1 single span with cost 1.50 (threshold 1.0) fires immediately (no min sample guard)
	batchCost := &tracewellv1.IngestBatch{
		ProjectId: "proj_guard",
		Traces: []*tracewellv1.Trace{
			{
				TraceId: "t_cost",
				Spans: []*tracewellv1.Span{
					{
						Status: tracewellv1.Status_STATUS_OK,
						Generation: &tracewellv1.Generation{
							Usage: &tracewellv1.Usage{
								CostUsd: 1.50,
							},
						},
					},
				},
			},
		},
	}

	if err := engine.ProcessBatch(ctx, batchCost); err != nil {
		t.Fatalf("unexpected error processing cost batch: %v", err)
	}

	eventsAfterCost := store.getEvents()
	if len(eventsAfterCost) != 3 {
		t.Fatalf("expected cost_usd to fire without min-sample guard, got %d events total", len(eventsAfterCost))
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

	// 1. Send burst with 5 spans (4 errors = 80% error rate > 50%) at t=0s -> should FIRE
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

func TestDeletedRuleFilter(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store := &mockStore{
		rules: []model.AlertRule{
			{
				ID:        1,
				ProjectID: "proj_filter",
				Name:      "active rule",
				Metric:    "error_rate",
				Enabled:   true,
			},
			{
				ID:        2,
				ProjectID: "proj_filter",
				Name:      "disabled rule",
				Metric:    "error_rate",
				Enabled:   false,
			},
			{
				ID:        3,
				ProjectID: "proj_filter",
				Name:      "soft-deleted rule",
				Metric:    "error_rate",
				Enabled:   true,
				DeletedAt: &now,
			},
		},
	}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewAlertEngine(store, dispatcher, logger, nil)

	if err := engine.ReloadRules(ctx); err != nil {
		t.Fatalf("failed reloading rules: %v", err)
	}

	activeRules := engine.GetRules()
	if len(activeRules) != 1 {
		t.Fatalf("expected only 1 active rule (ignoring disabled and deleted), got %d", len(activeRules))
	}
	if activeRules[0].ID != 1 {
		t.Errorf("expected active rule ID 1, got %d", activeRules[0].ID)
	}
}

func TestDeliveryFailureStillPersistsAlertEvent(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	store := &mockStore{}
	dispatcher := &mockDispatcher{
		deliveryErr: errors.New("slack webhook failure"),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng := NewAlertEngine(store, dispatcher, logger, func() time.Time { return now })
	eng.SetRules([]model.AlertRule{{
		ID: 1, ProjectID: "proj_dev", Name: "errors", Metric: "error_rate",
		Threshold: 0.1, WindowSeconds: 60, CooldownSeconds: 300, Channel: "slack", Enabled: true,
	}})

	// 5 error spans to satisfy min sample guard
	batch := &tracewellv1.IngestBatch{
		ProjectId: "proj_dev",
		Traces: []*tracewellv1.Trace{{
			Spans: []*tracewellv1.Span{
				{Status: tracewellv1.Status_STATUS_ERROR},
				{Status: tracewellv1.Status_STATUS_ERROR},
				{Status: tracewellv1.Status_STATUS_ERROR},
				{Status: tracewellv1.Status_STATUS_ERROR},
				{Status: tracewellv1.Status_STATUS_ERROR},
			},
		}},
	}

	var mBefore dto.Metric
	_ = metrics.AlertsFired.WithLabelValues("error_rate", "slack").Write(&mBefore)
	firedBefore := mBefore.GetCounter().GetValue()

	// ProcessBatch should succeed because delivery failure is non-fatal to the ingest pipeline
	if err := eng.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("ProcessBatch should not fail on delivery error: %v", err)
	}

	// AlertEvent must still be written to Postgres!
	if got := len(store.getEvents()); got != 1 {
		t.Fatalf("expected 1 persisted alert event despite delivery failure, got %d", got)
	}

	var mAfter dto.Metric
	_ = metrics.AlertsFired.WithLabelValues("error_rate", "slack").Write(&mAfter)
	firedAfter := mAfter.GetCounter().GetValue()

	if firedAfter-firedBefore != 1 {
		t.Errorf("expected alerter_alerts_fired_total to increment by 1, before=%f after=%f", firedBefore, firedAfter)
	}
}

func TestEvaluateAll(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	store := &mockStore{}
	dispatcher := &mockDispatcher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng := NewAlertEngine(store, dispatcher, logger, func() time.Time { return now })

	eng.SetRules([]model.AlertRule{
		{
			ID: 10, ProjectID: "proj_a", Name: "cost a", Metric: "cost_usd",
			Threshold: 1.0, WindowSeconds: 60, CooldownSeconds: 60, Channel: "log", Enabled: true,
		},
		{
			ID: 20, ProjectID: "proj_b", Name: "cost b", Metric: "cost_usd",
			Threshold: 2.0, WindowSeconds: 60, CooldownSeconds: 60, Channel: "log", Enabled: true,
		},
	})

	// Add window records directly to window tracker
	eng.Windows().AddSpan("proj_a", false, 1.50, now) // Crosses threshold 1.0
	eng.Windows().AddSpan("proj_b", false, 0.50, now) // Below threshold 2.0

	if err := eng.EvaluateAll(ctx, now); err != nil {
		t.Fatalf("EvaluateAll failed: %v", err)
	}

	events := store.getEvents()
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event (for proj_a), got %d", len(events))
	}
	if events[0].RuleID != 10 || events[0].ProjectID != "proj_a" {
		t.Errorf("unexpected event: %+v", events[0])
	}
}
