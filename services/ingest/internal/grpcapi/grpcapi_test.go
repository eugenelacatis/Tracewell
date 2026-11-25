package grpcapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest/ingesttest"
)

// newClient serves IngestService over an in-memory connection and returns a client for it.
func newClient(t *testing.T, p *ingesttest.Producer) tracewellv1.IngestServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := NewServer(ingesttest.NewService(p), slog.New(slog.NewTextHandler(io.Discard, nil)))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return tracewellv1.NewIngestServiceClient(conn)
}

func contractBatch(t *testing.T) *tracewellv1.IngestBatch {
	t.Helper()
	b := &tracewellv1.IngestBatch{}
	if err := protojson.Unmarshal([]byte(ingesttest.ContractExample), b); err != nil {
		t.Fatal(err)
	}
	return b
}

// call sends batch with key (none if empty) and returns the response, error and trailer.
func call(c tracewellv1.IngestServiceClient, key string, batch *tracewellv1.IngestBatch) (*tracewellv1.IngestResponse, error, metadata.MD) {
	ctx := context.Background()
	if key != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	}
	var trailer metadata.MD
	resp, err := c.Ingest(ctx, batch, grpc.Trailer(&trailer))
	return resp, err, trailer
}

func TestAcceptsContractExample(t *testing.T) {
	p := &ingesttest.Producer{}
	resp, err, _ := call(newClient(t, p), ingesttest.GoodKey, contractBatch(t))
	if err != nil || resp.Accepted != 1 {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	records := p.Records()
	if len(records) != 1 || records[0].Key != "proj_a" {
		t.Fatalf("records = %+v", records)
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(records[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.IdempotencyKey != "9f2c:17" || got.BatchSeq != 17 || got.Traces[0].Spans[0].Generation.Usage.TotalTokens != 12 {
		t.Fatalf("batch = %v", &got)
	}
}

func TestOverwritesClientProjectID(t *testing.T) {
	p := &ingesttest.Producer{}
	batch := &tracewellv1.IngestBatch{
		IdempotencyKey: "k:1",
		ProjectId:      "proj_evil",
		Traces:         []*tracewellv1.Trace{{TraceId: "t1", ProjectId: "proj_evil"}},
	}
	if _, err, _ := call(newClient(t, p), ingesttest.GoodKey, batch); err != nil {
		t.Fatal(err)
	}
	var got tracewellv1.IngestBatch
	if err := proto.Unmarshal(p.Records()[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.ProjectId != "proj_a" || got.Traces[0].ProjectId != "proj_a" {
		t.Fatalf("project ids = %q %q", got.ProjectId, got.Traces[0].ProjectId)
	}
}

func TestRejections(t *testing.T) {
	huge := &tracewellv1.IngestBatch{
		IdempotencyKey: "k:1",
		Traces:         []*tracewellv1.Trace{{TraceId: "t1", Name: strings.Repeat("x", ingest.MaxBatchBytes)}},
	}
	tests := []struct {
		name, key  string
		batch      *tracewellv1.IngestBatch
		code       codes.Code
		retryAfter string // expected retry-after trailer; only the rate limit sets one
	}{
		{"missing key", "", nil, codes.Unauthenticated, ""},
		{"bad key", "tw_wrong", nil, codes.Unauthenticated, ""},
		{"rate limited", ingesttest.LimitedKey, nil, codes.ResourceExhausted, "1"},
		{"postgres down", ingesttest.PGDownKey, nil, codes.Unavailable, ""},
		{"missing idempotency key", ingesttest.GoodKey, &tracewellv1.IngestBatch{}, codes.InvalidArgument, ""},
		{"too large", ingesttest.GoodKey, huge, codes.ResourceExhausted, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.batch == nil {
				tt.batch = contractBatch(t)
			}
			p := &ingesttest.Producer{}
			_, err, trailer := call(newClient(t, p), tt.key, tt.batch)
			if status.Code(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tt.code)
			}
			if got := strings.Join(trailer.Get("retry-after"), ","); got != tt.retryAfter {
				t.Fatalf("retry-after trailer = %q, want %q", got, tt.retryAfter)
			}
			if len(p.Records()) != 0 {
				t.Fatalf("produced %d records on a rejected request", len(p.Records()))
			}
		})
	}
}

func TestRedisDownFailsOpen(t *testing.T) {
	p := &ingesttest.Producer{}
	if _, err, _ := call(newClient(t, p), ingesttest.RedisDown, contractBatch(t)); err != nil || len(p.Records()) != 1 {
		t.Fatalf("err=%v records=%d", err, len(p.Records()))
	}
}

func TestKafkaFailureIsUnavailable(t *testing.T) {
	p := &ingesttest.Producer{Err: errors.New("broker down")}
	if _, err, _ := call(newClient(t, p), ingesttest.GoodKey, contractBatch(t)); status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want UNAVAILABLE", err)
	}
}
