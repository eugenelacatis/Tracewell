// Package ingesttest has the fakes and fixtures shared by the HTTP and gRPC transport tests, so both
// run the same auth, validation and rate-limit cases against the same ingest.Service.
package ingesttest

import (
	"context"
	"errors"
	"sync"

	"github.com/eugenelacatis/tracewell/services/ingest/internal/auth"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
)

// Keys accepted by NewService.
const (
	GoodKey    = "tw_good"      // project proj_a
	LimitedKey = "tw_limited"   // project proj_limited, always over its rate limit
	RedisDown  = "tw_redisdown" // project proj_redisdown, whose rate-limit check errors
	PGDownKey  = "pgdown"       // the key lookup fails as if Postgres were down
)

type Keys map[string]string // key -> project

func (k Keys) ProjectID(_ context.Context, key string) (string, error) {
	if key == PGDownKey {
		return "", errors.New("connection refused")
	}
	if pid, ok := k[key]; ok {
		return pid, nil
	}
	return "", auth.ErrUnknownKey
}

// Limiter rejects proj_limited and errors for proj_redisdown.
type Limiter struct{}

func (Limiter) Allow(_ context.Context, projectID string) (bool, error) {
	switch projectID {
	case "proj_limited":
		return false, nil
	case "proj_redisdown":
		return false, errors.New("dial tcp: connection refused")
	}
	return true, nil
}

type Record struct {
	Key   string
	Value []byte
}

// Producer records what was produced. Set Err to make every produce fail.
type Producer struct {
	Err error

	mu      sync.Mutex
	records []Record
}

func (p *Producer) Produce(_ context.Context, key string, value []byte) error {
	if p.Err != nil {
		return p.Err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, Record{key, value})
	return nil
}

func (p *Producer) Records() []Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Record(nil), p.records...)
}

func NewService(p *Producer) *ingest.Service {
	return &ingest.Service{
		Keys:     Keys{GoodKey: "proj_a", LimitedKey: "proj_limited", RedisDown: "proj_redisdown"},
		Limiter:  Limiter{},
		Producer: p,
	}
}

// ContractExample is the example request body from docs/contracts/http.md, so the contract and the
// server can't drift.
const ContractExample = `{
  "idempotencyKey": "9f2c:17",
  "sdkInstanceId": "9f2c",
  "batchSeq": "17",
  "traces": [{
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "sessionId": "chat-42",
    "userId": "user-7",
    "name": "chat turn",
    "tags": ["example"],
    "startTime": "2026-10-26T17:04:05.000000Z",
    "endTime": "2026-10-26T17:04:06.200000Z",
    "spans": [{
      "spanId": "00f067aa0ba902b7",
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "parentSpanId": "",
      "name": "openai.chat",
      "kind": "SPAN_KIND_GENERATION",
      "startTime": "2026-10-26T17:04:05.100000Z",
      "endTime": "2026-10-26T17:04:06.100000Z",
      "status": "STATUS_OK",
      "generation": {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt": "[{\"role\":\"user\",\"content\":\"hi\"}]",
        "completion": "Hello!",
        "params": { "temperature": 0.2 },
        "usage": { "promptTokens": 9, "completionTokens": 3, "totalTokens": 12, "costUsd": 0.0000045 }
      },
      "metadata": {}
    }]
  }]
}`
