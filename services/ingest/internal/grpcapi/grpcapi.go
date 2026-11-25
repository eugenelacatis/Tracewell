// Package grpcapi serves tracewell.v1.IngestService/Ingest, the gRPC twin of POST /v1/ingest
// (docs/contracts/http.md, gRPC). Auth, rate limiting, validation and the Kafka write are the shared
// ones in internal/ingest; this package only reads metadata and maps errors to gRPC status codes.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/ingest/internal/ingest"
)

// NewServer returns a gRPC server with IngestService registered. Messages over the contract's 5 MB
// are refused by grpc-go itself with RESOURCE_EXHAUSTED and no retry-after trailer, which is the
// contract's "too large" case.
func NewServer(svc *ingest.Service, logger *slog.Logger) *grpc.Server {
	s := grpc.NewServer(grpc.MaxRecvMsgSize(ingest.MaxBatchBytes))
	tracewellv1.RegisterIngestServiceServer(s, &server{svc: svc, logger: logger})
	return s
}

type server struct {
	tracewellv1.UnimplementedIngestServiceServer
	svc    *ingest.Service
	logger *slog.Logger
}

func (s *server) Ingest(ctx context.Context, batch *tracewellv1.IngestBatch) (*tracewellv1.IngestResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	log := s.logger
	if id := first(md, "x-request-id"); id != "" {
		log = log.With("request_id", id)
	}

	projectID, err := s.svc.Authorize(ctx, log, first(md, "authorization"))
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	accepted, err := s.svc.Write(ctx, log, projectID, batch)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &tracewellv1.IngestResponse{Accepted: uint32(accepted)}, nil
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

// toStatus maps an *ingest.Error to the gRPC code in the contract. A rate limit also sets the
// retry-after trailer, which is how clients tell it apart from a too-large message.
func toStatus(ctx context.Context, err error) error {
	var e *ingest.Error
	if !errors.As(err, &e) {
		return status.Error(codes.Unavailable, "internal error")
	}
	switch e.Code {
	case ingest.InvalidArgument:
		return status.Error(codes.InvalidArgument, e.Msg)
	case ingest.Unauthorized:
		return status.Error(codes.Unauthenticated, e.Msg)
	case ingest.RateLimited:
		grpc.SetTrailer(ctx, metadata.Pairs("retry-after", strconv.Itoa(ingest.RetryAfterSeconds)))
		return status.Error(codes.ResourceExhausted, e.Msg)
	default: // Unavailable, Internal: every 5xx is UNAVAILABLE, so clients retry
		return status.Error(codes.Unavailable, e.Msg)
	}
}
