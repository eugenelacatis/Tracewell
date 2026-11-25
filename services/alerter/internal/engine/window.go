package engine

import (
	"math"
	"sort"
	"sync"
	"time"
)

// SpanRecord represents a single span's error status, cost, and occurrence timestamp.
type SpanRecord struct {
	Timestamp time.Time
	IsError   bool
	CostUSD   float64
}

// TraceRecord represents a single trace's duration and occurrence timestamp.
type TraceRecord struct {
	Timestamp  time.Time
	DurationMs float64
}

// ProjectWindow holds raw windowed records for a single project.
type ProjectWindow struct {
	Spans  []SpanRecord
	Traces []TraceRecord
}

// WindowTracker maintains in-memory records per project for windowed sliding aggregations.
type WindowTracker struct {
	mu      sync.RWMutex
	windows map[string]*ProjectWindow // project_id -> ProjectWindow
}

// NewWindowTracker initializes an empty WindowTracker.
func NewWindowTracker() *WindowTracker {
	return &WindowTracker{
		windows: make(map[string]*ProjectWindow),
	}
}

func (w *WindowTracker) getOrCreateWindow(projectID string) *ProjectWindow {
	win, ok := w.windows[projectID]
	if !ok {
		win = &ProjectWindow{}
		w.windows[projectID] = win
	}
	return win
}

// AddSpan records a single span for a project.
func (w *WindowTracker) AddSpan(projectID string, isError bool, costUSD float64, ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	win := w.getOrCreateWindow(projectID)
	win.Spans = append(win.Spans, SpanRecord{
		Timestamp: ts,
		IsError:   isError,
		CostUSD:   costUSD,
	})
}

// AddSpans records multiple spans at once for a project.
func (w *WindowTracker) AddSpans(projectID string, total int, errors int, ts time.Time) {
	if total <= 0 {
		return
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	win := w.getOrCreateWindow(projectID)
	for i := 0; i < errors; i++ {
		win.Spans = append(win.Spans, SpanRecord{
			Timestamp: ts,
			IsError:   true,
		})
	}
	for i := 0; i < (total - errors); i++ {
		win.Spans = append(win.Spans, SpanRecord{
			Timestamp: ts,
			IsError:   false,
		})
	}
}

// AddTrace records a single trace duration for a project.
func (w *WindowTracker) AddTrace(projectID string, durationMs float64, ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	win := w.getOrCreateWindow(projectID)
	win.Traces = append(win.Traces, TraceRecord{
		Timestamp:  ts,
		DurationMs: durationMs,
	})
}

// CalculateErrorRate computes the error rate (error spans / total spans) for a project
// within the time window [now - windowSeconds, now].
// Enforces minimum sample guard of 5 spans: valid is true only when total >= 5.
func (w *WindowTracker) CalculateErrorRate(projectID string, now time.Time, windowSeconds int) (rate float64, total int64, errors int64, valid bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	win, ok := w.windows[projectID]
	if !ok || len(win.Spans) == 0 {
		return 0.0, 0, 0, false
	}

	windowStart := now.Add(-time.Duration(windowSeconds) * time.Second)

	for _, r := range win.Spans {
		if (r.Timestamp.Equal(windowStart) || r.Timestamp.After(windowStart)) && (r.Timestamp.Equal(now) || r.Timestamp.Before(now)) {
			total++
			if r.IsError {
				errors++
			}
		}
	}

	if total == 0 {
		return 0.0, 0, 0, false
	}

	rate = float64(errors) / float64(total)
	valid = total >= 5
	return rate, total, errors, valid
}

// CalculateCostUSD sums generation costUsd within the time window [now - windowSeconds, now].
// Does not require a minimum sample guard.
func (w *WindowTracker) CalculateCostUSD(projectID string, now time.Time, windowSeconds int) float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	win, ok := w.windows[projectID]
	if !ok || len(win.Spans) == 0 {
		return 0.0
	}

	windowStart := now.Add(-time.Duration(windowSeconds) * time.Second)
	var totalCost float64

	for _, r := range win.Spans {
		if (r.Timestamp.Equal(windowStart) || r.Timestamp.After(windowStart)) && (r.Timestamp.Equal(now) || r.Timestamp.Before(now)) {
			totalCost += r.CostUSD
		}
	}

	return totalCost
}

// CalculateP95Latency calculates the 95th percentile trace duration (ms) within [now - windowSeconds, now].
// Enforces minimum sample guard of 5 traces: valid is true only when count >= 5.
func (w *WindowTracker) CalculateP95Latency(projectID string, now time.Time, windowSeconds int) (p95 float64, count int64, valid bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	win, ok := w.windows[projectID]
	if !ok || len(win.Traces) == 0 {
		return 0.0, 0, false
	}

	windowStart := now.Add(-time.Duration(windowSeconds) * time.Second)
	var durations []float64

	for _, r := range win.Traces {
		if (r.Timestamp.Equal(windowStart) || r.Timestamp.After(windowStart)) && (r.Timestamp.Equal(now) || r.Timestamp.Before(now)) {
			durations = append(durations, r.DurationMs)
		}
	}

	count = int64(len(durations))
	if count < 5 {
		return 0.0, count, false
	}

	sort.Float64s(durations)
	// Nearest rank method for 95th percentile: rank = ceil(0.95 * N)
	rank := int(math.Ceil(0.95 * float64(len(durations))))
	idx := rank - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(durations) {
		idx = len(durations) - 1
	}

	return durations[idx], count, true
}

// GetProjects returns all project IDs currently held in tracker.
func (w *WindowTracker) GetProjects() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	projects := make([]string, 0, len(w.windows))
	for pid := range w.windows {
		projects = append(projects, pid)
	}
	return projects
}

// PruneOlderThan removes records across all projects older than cutoff.
func (w *WindowTracker) PruneOlderThan(cutoff time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for pid, win := range w.windows {
		var filteredSpans []SpanRecord
		for _, s := range win.Spans {
			if s.Timestamp.After(cutoff) {
				filteredSpans = append(filteredSpans, s)
			}
		}
		var filteredTraces []TraceRecord
		for _, t := range win.Traces {
			if t.Timestamp.After(cutoff) {
				filteredTraces = append(filteredTraces, t)
			}
		}
		if len(filteredSpans) == 0 && len(filteredTraces) == 0 {
			delete(w.windows, pid)
		} else {
			win.Spans = filteredSpans
			win.Traces = filteredTraces
		}
	}
}

// Clear resets in-memory records.
func (w *WindowTracker) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.windows = make(map[string]*ProjectWindow)
}
