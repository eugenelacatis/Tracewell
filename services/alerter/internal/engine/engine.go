package engine

import (
	"context"
	"fmt"
	"log/slog"
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

// Clock returns the configured clock function.
func (e *AlertEngine) Clock() Clock {
	return e.clock
}

// Windows returns the underlying WindowTracker.
func (e *AlertEngine) Windows() *WindowTracker {
	return e.windows
}

// SetRules updates active in-memory rules and prunes state for deleted/disabled rules.
func (e *AlertEngine) SetRules(rules []model.AlertRule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	newRuleMap := make(map[int64]model.AlertRule, len(rules))
	for _, r := range rules {
		// Ignore disabled rules and soft-deleted rules
		if r.Enabled && r.DeletedAt == nil {
			newRuleMap[r.ID] = r
		}
	}

	e.rules = newRuleMap
	metrics.RulesLoaded.Set(float64(len(newRuleMap)))

	// Clean up lastFired for rules that are no longer active
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

// ProcessBatch processes an IngestBatch from traces.raw, populates windows, and evaluates rules.
func (e *AlertEngine) ProcessBatch(ctx context.Context, batch *tracewellv1.IngestBatch) error {
	now := e.clock()
	batchProjID := batch.ProjectId
	seenProjects := make(map[string]struct{})

	for _, trace := range batch.Traces {
		projID := batchProjID
		if projID == "" {
			projID = trace.ProjectId
		}
		if projID == "" {
			continue
		}
		seenProjects[projID] = struct{}{}

		traceTs := now
		if trace.StartTime != nil {
			traceTs = trace.StartTime.AsTime()
		}

		var durationMs float64
		if trace.StartTime != nil && trace.EndTime != nil {
			dur := trace.EndTime.AsTime().Sub(trace.StartTime.AsTime())
			durationMs = float64(dur.Milliseconds())
			if durationMs < 0 {
				durationMs = 0
			}
		}
		e.windows.AddTrace(projID, durationMs, traceTs)

		for _, span := range trace.Spans {
			metrics.SpansProcessed.Inc()
			isError := span.Status == tracewellv1.Status_STATUS_ERROR
			if isError {
				metrics.ErrorSpansProcessed.Inc()
			}

			var costUSD float64
			if span.Generation != nil && span.Generation.Usage != nil {
				costUSD = span.Generation.Usage.CostUsd
			}

			spanTs := traceTs
			if span.StartTime != nil {
				spanTs = span.StartTime.AsTime()
			}

			e.windows.AddSpan(projID, isError, costUSD, spanTs)
		}
	}

	for pid := range seenProjects {
		if err := e.EvaluateRulesForProject(ctx, pid, now); err != nil {
			e.logger.Error("failed evaluating rules for project",
				"project_id", pid,
				"error", err,
			)
			return err
		}
	}

	// Prune windows older than 1 hour to prevent unbounded memory growth
	e.windows.PruneOlderThan(now.Add(-1 * time.Hour))

	return nil
}

// EvaluateAll evaluates all active rules across all known projects.
func (e *AlertEngine) EvaluateAll(ctx context.Context, now time.Time) error {
	projects := e.windows.GetProjects()
	e.mu.RLock()
	for _, r := range e.rules {
		projects = append(projects, r.ProjectID)
	}
	e.mu.RUnlock()

	seen := make(map[string]bool)
	for _, pid := range projects {
		if pid == "" || seen[pid] {
			continue
		}
		seen[pid] = true
		if err := e.EvaluateRulesForProject(ctx, pid, now); err != nil {
			e.logger.Error("periodic rule evaluation failed for project",
				"project_id", pid,
				"error", err,
			)
		}
	}
	return nil
}

// EvaluateRulesForProject evaluates all active rules for a given project at time now.
func (e *AlertEngine) EvaluateRulesForProject(ctx context.Context, projectID string, now time.Time) error {
	e.mu.RLock()
	var applicableRules []model.AlertRule
	for _, r := range e.rules {
		if r.ProjectID == projectID {
			applicableRules = append(applicableRules, r)
		}
	}
	e.mu.RUnlock()

	for _, rule := range applicableRules {
		var shouldFire bool
		var value float64

		switch rule.Metric {
		case "error_rate":
			rate, _, _, valid := e.windows.CalculateErrorRate(projectID, now, rule.WindowSeconds)
			// Enforces 5-sample guard and strict threshold crossing
			if valid && rate > rule.Threshold {
				shouldFire = true
				value = rate
			}

		case "cost_usd":
			cost := e.windows.CalculateCostUSD(projectID, now, rule.WindowSeconds)
			if cost > rule.Threshold {
				shouldFire = true
				value = cost
			}

		case "p95_latency_ms":
			p95, _, valid := e.windows.CalculateP95Latency(projectID, now, rule.WindowSeconds)
			// Enforces 5-trace guard and strict threshold crossing
			if valid && p95 > rule.Threshold {
				shouldFire = true
				value = p95
			}
		}

		if shouldFire {
			if err := e.maybeFireAlert(ctx, rule, value, now); err != nil {
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
		"%s %.2f > %.2f over %ds",
		rule.Metric,
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

	// 1. Insert into alert_events table first
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

	// 2. Deliver to configured channel (log, webhook, slack)
	if err := e.dispatcher.Deliver(ctx, rule, event); err != nil {
		e.logger.Error("failed delivering alert",
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"channel", rule.Channel,
			"error", err,
		)
	}

	metrics.AlertsFired.WithLabelValues(rule.Metric, rule.Channel).Inc()

	return nil
}
