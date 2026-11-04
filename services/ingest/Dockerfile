# Build from the repo root, because the service imports the generated proto module in proto/gen/go:
#   docker build -f services/ingest/Dockerfile -t tracewell/ingest .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY proto/gen/go/ proto/gen/go/
COPY services/ingest/go.mod services/ingest/go.sum services/ingest/
RUN cd services/ingest && go mod download
COPY services/ingest/ services/ingest/
RUN cd services/ingest && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest

# alpine rather than distroless so compose healthchecks can run `wget -qO- localhost:8081/readyz`.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/ingest /ingest
EXPOSE 8081
ENTRYPOINT ["/ingest"]
