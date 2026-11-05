# Build from the repo root:
#   docker build -f services/alerter/Dockerfile -t tracewell/alerter .
FROM golang:1.26-alpine AS build

WORKDIR /src

# Copy proto Go package and service files
COPY proto/gen/go /src/proto/gen/go
COPY services/alerter/go.mod services/alerter/go.sum /src/services/alerter/

WORKDIR /src/services/alerter
RUN go mod download

COPY services/alerter/ /src/services/alerter/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/alerter ./cmd/alerter

# Runtime image: alpine for healthcheck support
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/alerter /alerter

EXPOSE 8083
ENTRYPOINT ["/alerter"]

