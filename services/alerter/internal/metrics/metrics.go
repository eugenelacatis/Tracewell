package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	SpansProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerter_spans_processed_total",
		Help: "Total number of spans processed by alerter",
	})

	ErrorSpansProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerter_error_spans_total",
		Help: "Total number of error spans (STATUS_ERROR) encountered by alerter",
	})

	AlertsFired = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerter_alerts_fired_total",
		Help: "Total number of alert rules triggered",
	}, []string{"rule_id", "project_id", "channel"})

	ActiveRules = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alerter_rules_active",
		Help: "Number of active enabled alert rules currently loaded in memory",
	})

	RuleReloads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerter_rule_reloads_total",
		Help: "Total count of alert rule reload cycles executed",
	})

	RuleReloadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerter_rule_reload_errors_total",
		Help: "Total count of errors encountered while reloading alert rules",
	})
)

