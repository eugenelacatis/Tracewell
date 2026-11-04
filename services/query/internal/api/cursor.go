package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

// Cursor marks where the last page ended. Pages are ordered by (start_time, trace_id) descending,
// so the next page is everything strictly "before" this pair. That's keyset pagination: unlike an
// OFFSET, it stays correct and fast while new traces keep arriving at the top.
type Cursor struct {
	StartTime time.Time
	TraceID   string
}

type cursorJSON struct {
	T  string `json:"t"`
	ID string `json:"id"`
}

var errBadCursor = errors.New("invalid cursor")

// EncodeCursor returns base64url JSON, e.g. {"t":"2026-10-26T17:04:05Z","id":"4bf9..."}.
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(cursorJSON{T: c.StartTime.UTC().Format(time.RFC3339Nano), ID: c.TraceID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, errBadCursor
	}
	var cj cursorJSON
	if err := json.Unmarshal(b, &cj); err != nil || cj.ID == "" {
		return Cursor{}, errBadCursor
	}
	t, err := time.Parse(time.RFC3339Nano, cj.T)
	if err != nil {
		return Cursor{}, errBadCursor
	}
	return Cursor{StartTime: t.UTC(), TraceID: cj.ID}, nil
}

const (
	defaultLimit = 50
	maxLimit     = 200
)

// ParseLimit reads the limit query param: empty means 50, values are clamped to 1..200, and
// anything that isn't an integer is an error.
func ParseLimit(s string) (int, error) {
	if s == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	return min(max(n, 1), maxLimit), nil
}
