# Build from the repo root, because the service imports the generated proto module in proto/gen/go:
#   docker build -f services/writer/Dockerfile -t tracewell/writer .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY proto/gen/go/ proto/gen/go/
COPY services/writer/go.mod services/writer/go.sum services/writer/
RUN cd services/writer && go mod download
COPY services/writer/ services/writer/
RUN cd services/writer && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/writer ./cmd/writer

# alpine rather than distroless so compose healthchecks can run `wget -qO- localhost:8084/readyz`.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/writer /writer
EXPOSE 8084
ENTRYPOINT ["/writer"]
