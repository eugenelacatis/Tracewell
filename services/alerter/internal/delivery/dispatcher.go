package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/model"
)

// Dispatcher defines the interface for delivering triggered alerts.
type Dispatcher interface {
	Deliver(ctx context.Context, rule model.AlertRule, event model.AlertEvent) error
}

// ChannelDispatcher dispatches alerts to configured channels (log, webhook).
type ChannelDispatcher struct {
	httpClient *http.Client
	logger     *slog.Logger
}

// NewChannelDispatcher creates a new dispatcher with standard HTTP client.
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
	}
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

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, rule.Target, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(req)
		if err != nil {
			d.logger.Error("webhook delivery failed",
				"rule_id", rule.ID,
				"project_id", rule.ProjectID,
				"target", rule.Target,
				"error", err,
			)
			return fmt.Errorf("post webhook to %s: %w", rule.Target, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			d.logger.Warn("webhook returned non-2xx status",
				"rule_id", rule.ID,
				"project_id", rule.ProjectID,
				"target", rule.Target,
				"status_code", resp.StatusCode,
			)
			return fmt.Errorf("webhook responded with status %d", resp.StatusCode)
		}

		d.logger.Info("webhook alert delivered",
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"target", rule.Target,
			"status_code", resp.StatusCode,
		)
		return nil

	default:
		// Unsupported or Round 2 channel (e.g. slack) - log fallback
		d.logger.Warn("unsupported channel, falling back to log",
			"channel", rule.Channel,
			"rule_id", rule.ID,
			"project_id", rule.ProjectID,
			"value", event.Value,
		)
		return nil
	}
}
