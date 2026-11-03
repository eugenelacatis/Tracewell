// Package model holds the JSON shapes query-svc returns. Field names and nesting follow
// docs/contracts/http.md exactly.
package model

import (
	"encoding/json"
	"strings"
	"time"
)

// Trace is one item of GET /v1/traces and the "trace" object of GET /v1/traces/{traceId}.
type Trace struct {
	TraceID     string   `json:"traceId"`
	Name        string   `json:"name"`
	SessionID   string   `json:"sessionId"`
	UserID      string   `json:"userId"`
	Tags        []string `json:"tags"`
	StartTime   Time     `json:"startTime"`
	EndTime     Time     `json:"endTime"`
	DurationMs  float64  `json:"durationMs"`
	Status      string   `json:"status"`
	SpanCount   uint32   `json:"spanCount"`
	TotalTokens uint64   `json:"totalTokens"`
	CostUsd     float64  `json:"costUsd"`
}

type Usage struct {
	PromptTokens     uint32  `json:"promptTokens"`
	CompletionTokens uint32  `json:"completionTokens"`
	TotalTokens      uint32  `json:"totalTokens"`
	CostUsd          float64 `json:"costUsd"`
}

type Generation struct {
	Provider   string          `json:"provider"`
	Model      string          `json:"model"`
	Prompt     string          `json:"prompt"`
	Completion string          `json:"completion"`
	Params     json.RawMessage `json:"params"`
	Usage      Usage           `json:"usage"`
}

type Span struct {
	SpanID       string          `json:"spanId"`
	ParentSpanID string          `json:"parentSpanId"`
	Name         string          `json:"name"`
	Kind         string          `json:"kind"`
	StartTime    Time            `json:"startTime"`
	EndTime      Time            `json:"endTime"`
	DurationMs   float64         `json:"durationMs"`
	Status       string          `json:"status"`
	ErrorMessage string          `json:"errorMessage"`
	Generation   *Generation     `json:"generation"`
	Metadata     json.RawMessage `json:"metadata"`
}

type Eval struct {
	SpanID    string  `json:"spanId"`
	Evaluator string  `json:"evaluator"`
	Score     float64 `json:"score"`
	Label     string  `json:"label"`
	Reasoning string  `json:"reasoning"`
	CreatedAt Time    `json:"createdAt"`
}

type TraceList struct {
	Traces     []Trace `json:"traces"`
	NextCursor *string `json:"nextCursor"`
}

type TraceDetail struct {
	Trace Trace  `json:"trace"`
	Spans []Span `json:"spans"`
	Evals []Eval `json:"evals"`
}

// Time marshals as RFC 3339 in UTC with trailing zeros trimmed ("2026-10-26T17:04:06.2Z").
type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format(time.RFC3339Nano))
}

// SpanRow is a raw row from tracewell.spans, before it becomes a Span.
type SpanRow struct {
	SpanID, ParentSpanID, Name, Kind string
	StartTime, EndTime               time.Time
	DurationMs                       float64
	Status, ErrorMessage             string
	Provider, Model                  string
	Prompt, Completion, Params       string
	PromptTokens, CompletionTokens   uint32
	TotalTokens                      uint32
	CostUsd                          float64
	Metadata                         string
}

// ToSpan applies the contract's mapping rules: enums lose any proto prefix, generation is null
// unless kind is GENERATION, and JSON columns are passed through as objects.
func (r SpanRow) ToSpan() Span {
	s := Span{
		SpanID:       r.SpanID,
		ParentSpanID: r.ParentSpanID,
		Name:         r.Name,
		Kind:         Enum(r.Kind, "SPAN_KIND_"),
		StartTime:    Time(r.StartTime),
		EndTime:      Time(r.EndTime),
		DurationMs:   r.DurationMs,
		Status:       Enum(r.Status, "STATUS_"),
		ErrorMessage: r.ErrorMessage,
		Metadata:     JSONObject(r.Metadata),
	}
	if s.Kind == "GENERATION" {
		s.Generation = &Generation{
			Provider:   r.Provider,
			Model:      r.Model,
			Prompt:     r.Prompt,
			Completion: r.Completion,
			Params:     JSONObject(r.Params),
			Usage: Usage{
				PromptTokens:     r.PromptTokens,
				CompletionTokens: r.CompletionTokens,
				TotalTokens:      r.TotalTokens,
				CostUsd:          r.CostUsd,
			},
		}
	}
	return s
}

// Enum returns a stored enum value in the contract's form: no proto prefix, and "UNSPECIFIED"
// when empty. ClickHouse should already hold unprefixed values; this keeps the API stable if a
// prefixed value slips through.
func Enum(v, prefix string) string {
	v = strings.TrimPrefix(v, prefix)
	if v == "" {
		return "UNSPECIFIED"
	}
	return v
}

// JSONObject turns a JSON column into a raw object, using {} for empty or malformed values so one
// bad row can't break the whole response.
func JSONObject(s string) json.RawMessage {
	if s == "" || !json.Valid([]byte(s)) {
		return json.RawMessage("{}")
	}
	return json.RawMessage(s)
}
