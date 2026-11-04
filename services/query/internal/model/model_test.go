package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnum(t *testing.T) {
	cases := []struct{ in, prefix, want string }{
		{"GENERATION", "SPAN_KIND_", "GENERATION"},
		{"SPAN_KIND_GENERATION", "SPAN_KIND_", "GENERATION"},
		{"OK", "STATUS_", "OK"},
		{"STATUS_ERROR", "STATUS_", "ERROR"},
		{"", "STATUS_", "UNSPECIFIED"},
		{"STATUS_UNSPECIFIED", "STATUS_", "UNSPECIFIED"},
	}
	for _, c := range cases {
		if got := Enum(c.in, c.prefix); got != c.want {
			t.Errorf("Enum(%q, %q) = %q, want %q", c.in, c.prefix, got, c.want)
		}
	}
}

func TestToSpanGenerationNull(t *testing.T) {
	s := SpanRow{SpanID: "a", Kind: "SPAN", Model: "ignored"}.ToSpan()
	if s.Generation != nil {
		t.Fatalf("generation should be nil for kind SPAN, got %+v", s.Generation)
	}
	b, _ := json.Marshal(s)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if v, ok := m["generation"]; !ok || v != nil {
		t.Fatalf(`want "generation": null in JSON, got %s`, b)
	}
	if string(s.Metadata) != "{}" {
		t.Fatalf("empty metadata should be {}, got %s", s.Metadata)
	}
}

func TestToSpanGeneration(t *testing.T) {
	s := SpanRow{
		Kind: "GENERATION", Status: "OK", Model: "gpt-4o-mini",
		Params: `{"temperature":0.2}`, PromptTokens: 9, CompletionTokens: 3, TotalTokens: 12,
	}.ToSpan()
	if s.Generation == nil {
		t.Fatal("generation should be set for kind GENERATION")
	}
	if s.Generation.Model != "gpt-4o-mini" || s.Generation.Usage.TotalTokens != 12 {
		t.Fatalf("unexpected generation %+v", s.Generation)
	}
	if string(s.Generation.Params) != `{"temperature":0.2}` {
		t.Fatalf("params not passed through: %s", s.Generation.Params)
	}
}

func TestJSONObjectMalformed(t *testing.T) {
	if got := string(JSONObject("{not json")); got != "{}" {
		t.Fatalf("got %s", got)
	}
}

func TestTimeJSON(t *testing.T) {
	ts := time.Date(2026, 10, 26, 17, 4, 6, 200000000, time.UTC)
	b, _ := json.Marshal(Time(ts))
	if string(b) != `"2026-10-26T17:04:06.2Z"` {
		t.Fatalf("got %s", b)
	}
}
