package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/delivery"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/metrics"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/store"
)

// Clock provides current time, allowing deterministic tests.
type Clock func() time.Time

// AlertEngine evaluates stream metrics over in-memory windows and triggers alerts.
type AlertEngine struct {
	store      store.Store
	dispatcher delivery.Dispatcher
	windows    *WindowTracker
	logger     *slog.Logger
	clock      Clock

	mu        sync.RWMutex
	rules     map[int64]model.AlertRule // rule_id -> AlertRule
	lastFired map[int64]time.Time       // rule_id -> time last fired
}

// NewAlertEngine creates a new AlertEngine.
func NewAlertEngine(
	s store.Store,
	disp delivery.Dispatcher,
	logger *slog.Logger,
	clock Clock,
) *AlertEngine {
	if logger == nil {
		logger = slog.Default()
	}
	if clock == nil {
		clock = time.Now
	}

	return &AlertEngine{
		store:      s,
		dispatcher: disp,
		windows:    NewWindowTracker(),
		logger:     logger,
		clock:      clock,
		rules:      make(map[int64]model.AlertRule),
		lastFired:  make(map[int64]time.Time),
	}
}

// SetRules updates active in-memory rules and prunes state for deleted rules.
func (e *AlertEngine) SetRules(rules []model.AlertRule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	newRuleMap := make(map[int64]model.AlertRule, len(rules))
	for _, r := range rules {
		if r.Enabled {
			newRuleMap[r.ID] = r
		}
	}

	e.rules = newRuleMap
	metrics.ActiveRules.Set(float64(len(newRuleMap)))

	// Clean up lastFired for rules that no longer exist
	for ruleID := range e.lastFired {
		if _, ok := newRuleMap[ruleID]; !ok {
			delete(e.lastFired, ruleID)
		}
	}
}

// GetRules returns a copy of current active rules.
func (e *AlertEngine) GetRules() []model.AlertRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	list := make([]model.AlertRule, 0, len(e.rules))
	for _, r := range e.rules {
		list = append(list, r)
	}
	return list
}

// ReloadRules fetches enabled rules from the store and refreshes in-memory state.
func (e *AlertEngine) ReloadRules(ctx context.Context) error {
	rules, err := e.store.LoadEnabledRules(ctx)
	if err != nil {
		metrics.RuleReloadErrors.Inc()
		return fmt.Errorf("reload rules from store: %w", err)
	}

	e.SetRules(rules)
	metrics.RuleReloads.Inc()
	e.logger.Info("alert rules reloaded", "active_count", len(rules))
	return nil
}

// ProcessBatch processes an IngestBatch from traces.raw and evaluates rules.
func (e *AlertEngine) ProcessBatch(ctx context.Context, batch *tracewellv1.IngestBatch) error {
	now := e.clock()

	// Aggregate spans by project_id
	projectSpans := make(map[string]struct{ total, errors int })

	for _, trace := range batch.Traces {
		projID := batch.ProjectId
		if projID == "" {
			projID = trace.ProjectId
		}
		if projID == "" {
			continue
		}

		for _, span := range trace.Spans {
			entry := projectSpans[projID]
			entry.total++
			if span.Status == tracewellv1.Status_STATUS_ERROR {
				entry.errors++
				metrics.ErrorSpansProcessed.Inc()
			}
			metrics.SpansProcessed.Inc()
			projectSpans[projID] = entry
		}
	}

	// Record spans into project windows and evaluate rules
	for projID, counts := range projectSpans {
		e.windows.AddSpans(projID, counts.total, counts.errors, now)
		if err := e.EvaluateRulesForProject(ctx, projID, now); err != nil {
			e.logger.Error("failed evaluating rules for project",
				"project_id", projID,
				"error", err,
			)
			return err
		}
	}

	// Prune windows older than 15 minutes to prevent memory growth
	e.windows.PruneOlderThan(now.Add(-15 * time.Minute))

	return nil
}

// EvaluateRulesForProject evaluates all active rules for a given project at time now.
func (e *AlertEngine) EvaluateRulesForProject(ctx context.Context, projectID string, now time.Time) error {
	e.mu.RLock()
	var applicableRules []model.AlertRule
	for _, r := range e.rules {
		if r.ProjectID == projectID && r.Metric == "error_rate" {
			applicableRules = append(applicableRules, r)
		}
	}
	e.mu.RUnlock()

	for _, rule := range applicableRules {
		rate, total, _ := e.windows.CalculateErrorRate(projectID, now, rule.WindowSeconds)
		if total == 0 {
			continue
		}

		// Fire when metric value strictly crosses threshold
		if rate > rule.Threshold {
			if err := e.maybeFireAlert(ctx, rule, rate, now); err != nil {
				return fmt.Errorf("maybe fire alert (rule %d): %w", rule.ID, err)
			}
		}
	}

	return nil
}

// maybeFireAlert checks cooldown and, if passed, inserts alert_events and delivers.
func (e *AlertEngine) maybeFireAlert(
	ctx context.Context,
	rule model.AlertRule,
	value float64,
	now time.Time,
) error {
	e.mu.Lock()
	lastTime, firedBefore := e.lastFired[rule.ID]
	cooldown := time.Duration(rule.CooldownSeconds) * time.Second

	if firedBefore && now.Sub(lastTime) < cooldown {
		// Inside cooldown period: suppress firing
		e.mu.Unlock()
		return nil
	}

	e.mu.Unlock()

	message := fmt.Sprintf(
		"Rule '%s' fired: error rate %.2f crossed threshold %.2f (window: %ds)",
		rule.Name,
		value,
		rule.Threshold,
		rule.WindowSeconds,
	)

	event := model.AlertEvent{
		RuleID:    rule.ID,
		ProjectID: rule.ProjectID,
		Value:     value,
		Message:   message,
		FiredAt:   now,
	}

	// 1. Insert into alert_events table
	if err := e.store.InsertAlertEvent(ctx, event); err != nil {
		e.logger.Error("failed inserting alert event to postgres",
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"error", err,
		)
		return fmt.Errorf("insert alert event: %w", err)
	}
	e.mu.Lock()
	e.lastFired[rule.ID] = now
	e.mu.Unlock()

	// 2. Deliver to configured channel (log / webhook)
	if err := e.dispatcher.Deliver(ctx, rule, event); err != nil {
		e.logger.Error("failed delivering alert",
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"channel", rule.Channel,
			"error", err,
		)
	}

	metrics.AlertsFired.WithLabelValues(
		strconv.FormatInt(rule.ID, 10),
		rule.ProjectID,
		rule.Channel,
	).Inc()

	return nil
}
