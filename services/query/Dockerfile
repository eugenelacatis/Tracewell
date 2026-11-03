# Build from the repo root:
#   docker build -f services/query/Dockerfile -t tracewell/query .
FROM golang:1.26 AS build
WORKDIR /src/services/query
COPY services/query/go.mod services/query/go.sum ./
RUN go mod download
COPY services/query/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/query ./cmd/query

# alpine rather than distroless so compose healthchecks can run `wget -qO- localhost:8082/readyz`.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/query /query
EXPOSE 8082
ENTRYPOINT ["/query"]
