package engine

import (
	"sync"
	"time"
)

// SpanRecord represents a single span's status and occurrence timestamp.
type SpanRecord struct {
	Timestamp time.Time
	IsError   bool
}

// WindowTracker maintains in-memory span records per project for windowed aggregation.
type WindowTracker struct {
	mu      sync.RWMutex
	records map[string][]SpanRecord // project_id -> records
}

// NewWindowTracker initializes an empty WindowTracker.
func NewWindowTracker() *WindowTracker {
	return &WindowTracker{
		records: make(map[string][]SpanRecord),
	}
}

// AddSpan records a single span for a project.
func (w *WindowTracker) AddSpan(projectID string, isError bool, ts time.Time) {
	if ts.IsZero() {
		ts = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.records[projectID] = append(w.records[projectID], SpanRecord{
		Timestamp: ts,
		IsError:   isError,
	})
}

// AddSpans records multiple spans at once for a project (e.g. from an IngestBatch).
func (w *WindowTracker) AddSpans(projectID string, total int, errors int, ts time.Time) {
	if total <= 0 {
		return
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 0; i < errors; i++ {
		w.records[projectID] = append(w.records[projectID], SpanRecord{
			Timestamp: ts,
			IsError:   true,
		})
	}
	for i := 0; i < (total - errors); i++ {
		w.records[projectID] = append(w.records[projectID], SpanRecord{
			Timestamp: ts,
			IsError:   false,
		})
	}
}

// CalculateErrorRate computes the error rate (error spans / total spans) for a project
// within the time window [now - windowSeconds, now].
// If total spans in the window is 0, returns (0.0, 0, 0).
func (w *WindowTracker) CalculateErrorRate(projectID string, now time.Time, windowSeconds int) (rate float64, total int64, errors int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	records, ok := w.records[projectID]
	if !ok || len(records) == 0 {
		return 0.0, 0, 0
	}

	windowStart := now.Add(-time.Duration(windowSeconds) * time.Second)

	for _, r := range records {
		// Include records within [windowStart, now]
		if (r.Timestamp.Equal(windowStart) || r.Timestamp.After(windowStart)) && (r.Timestamp.Equal(now) || r.Timestamp.Before(now)) {
			total++
			if r.IsError {
				errors++
			}
		}
	}

	if total == 0 {
		return 0.0, 0, 0
	}

	return float64(errors) / float64(total), total, errors
}

// PruneOlderThan removes records across all projects older than cutoff.
func (w *WindowTracker) PruneOlderThan(cutoff time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for projectID, records := range w.records {
		var filtered []SpanRecord
		for _, r := range records {
			if r.Timestamp.After(cutoff) {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) == 0 {
			delete(w.records, projectID)
		} else {
			w.records[projectID] = filtered
		}
	}
}

// Clear resets in-memory records.
func (w *WindowTracker) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = make(map[string][]SpanRecord)
}
