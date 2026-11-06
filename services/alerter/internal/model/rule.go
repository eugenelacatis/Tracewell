package model

import "time"

// AlertRule represents a row from postgres alert_rules table.
type AlertRule struct {
	ID              int64     `json:"id"`
	ProjectID       string    `json:"projectId"`
	Name            string    `json:"name"`
	Metric          string    `json:"metric"` // 'error_rate', 'cost_usd', 'p95_latency_ms'
	Threshold       float64   `json:"threshold"`
	WindowSeconds   int       `json:"windowSeconds"`
	CooldownSeconds int       `json:"cooldownSeconds"`
	Channel         string    `json:"channel"` // 'log', 'webhook', 'slack'
	Target          string    `json:"target"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

// AlertEvent represents a row written to postgres alert_events table when an alert fires.
type AlertEvent struct {
	ID        int64     `json:"id"`
	RuleID    int64     `json:"ruleId"`
	ProjectID string    `json:"projectId"`
	Value     float64   `json:"value"`
	Message   string    `json:"message"`
	FiredAt   time.Time `json:"firedAt"`
}

// WebhookPayload is the JSON structure posted to webhook targets.
type WebhookPayload struct {
	RuleID    int64     `json:"rule_id"`
	RuleName  string    `json:"rule_name"`
	ProjectID string    `json:"project_id"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Message   string    `json:"message"`
	FiredAt   time.Time `json:"fired_at"`
}

