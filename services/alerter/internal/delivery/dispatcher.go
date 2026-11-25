package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/metrics"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
)

// DefaultBackoff provides 3 retries with exponential backoff.
var DefaultBackoff = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}

// Dispatcher defines the interface for delivering triggered alerts.
type Dispatcher interface {
	Deliver(ctx context.Context, rule model.AlertRule, event model.AlertEvent) error
}

// ChannelDispatcher dispatches alerts to configured channels (log, webhook, slack) with retries.
type ChannelDispatcher struct {
	httpClient *http.Client
	logger     *slog.Logger
	backoff    []time.Duration
	sleep      func(time.Duration)
}

// NewChannelDispatcher creates a new dispatcher with standard HTTP client and default retries.
func NewChannelDispatcher(logger *slog.Logger, client *http.Client) *ChannelDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ChannelDispatcher{
		httpClient: client,
		logger:     logger,
		backoff:    DefaultBackoff,
		sleep:      time.Sleep,
	}
}

// SetBackoff overrides the retry backoff sequence (useful in unit tests).
func (d *ChannelDispatcher) SetBackoff(backoff []time.Duration) {
	d.backoff = backoff
}

// SetSleep overrides the sleep function (useful in unit tests).
func (d *ChannelDispatcher) SetSleep(fn func(time.Duration)) {
	d.sleep = fn
}

// Deliver routes the alert event to the rule's specified channel.
func (d *ChannelDispatcher) Deliver(ctx context.Context, rule model.AlertRule, event model.AlertEvent) error {
	switch rule.Channel {
	case "log":
		d.logger.Info("alert fired",
			"rule_id", rule.ID,
			"rule_name", rule.Name,
			"project_id", rule.ProjectID,
			"metric", rule.Metric,
			"value", event.Value,
			"threshold", rule.Threshold,
			"message", event.Message,
		)
		return nil

	case "webhook":
		payload := model.WebhookPayload{
			RuleID:    rule.ID,
			RuleName:  rule.Name,
			ProjectID: rule.ProjectID,
			Metric:    rule.Metric,
			Value:     event.Value,
			Threshold: rule.Threshold,
			Message:   event.Message,
			FiredAt:   event.FiredAt,
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal webhook payload: %w", err)
		}

		return d.postWithRetry(ctx, rule.Channel, rule.Target, bodyBytes)

	case "slack":
		windowStart := event.FiredAt.Add(-time.Duration(rule.WindowSeconds) * time.Second)
		fromStr := url.QueryEscape(windowStart.UTC().Format(time.RFC3339))
		toStr := url.QueryEscape(event.FiredAt.UTC().Format(time.RFC3339))
		traceURL := fmt.Sprintf("http://localhost:3000/traces?from=%s&to=%s", fromStr, toStr)

		text := fmt.Sprintf(
			"Rule '%s' fired for project '%s': %s value %.2f crossed threshold %.2f (window: %ds). View traces: %s",
			rule.Name,
			rule.ProjectID,
			rule.Metric,
			event.Value,
			rule.Threshold,
			rule.WindowSeconds,
			traceURL,
		)

		payload := model.SlackPayload{
			Text: text,
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal slack payload: %w", err)
		}

		return d.postWithRetry(ctx, rule.Channel, rule.Target, bodyBytes)

	default:
		d.logger.Warn("unsupported channel, falling back to log",
			"channel", rule.Channel,
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"value", event.Value,
		)
		return nil
	}
}

// postWithRetry delivers a JSON payload to the target URL, retrying up to len(backoff) times.
func (d *ChannelDispatcher) postWithRetry(ctx context.Context, channel, target string, body []byte) error {
	attempts := 1 + len(d.backoff)
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			backoffDur := d.backoff[attempt-1]
			if d.sleep != nil {
				d.sleep(backoffDur)
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			lastErr = fmt.Errorf("create request: %w", err)
			break
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("post to %s: %w", target, err)
			d.logger.Warn("delivery attempt failed",
				"channel", channel,
				"target", target,
				"attempt", attempt+1,
				"error", err,
			)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("target responded with status %d", resp.StatusCode)
			d.logger.Warn("delivery attempt returned non-2xx status",
				"channel", channel,
				"target", target,
				"attempt", attempt+1,
				"status_code", resp.StatusCode,
			)
			continue
		}
		_ = resp.Body.Close()

		d.logger.Info("alert delivered successfully",
			"channel", channel,
			"target", target,
			"status_code", resp.StatusCode,
		)
		return nil
	}

	metrics.DeliveryErrors.WithLabelValues(channel).Inc()
	d.logger.Error("all delivery attempts failed",
		"channel", channel,
		"target", target,
		"attempts", attempts,
		"error", lastErr,
	)
	return lastErr
}
