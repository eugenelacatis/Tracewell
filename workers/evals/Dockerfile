# Build from the repo root:
#   docker build -f workers/evals/Dockerfile -t tracewell/evals .
FROM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY --from=uv_bin /uv /uvx /bin/

WORKDIR /app

# Copy proto python package and workers/evals code
COPY proto/gen/python /app/proto/gen/python
COPY workers/evals /app/workers/evals

WORKDIR /app/workers/evals
RUN uv sync --no-dev

ENV PATH="/app/workers/evals/.venv/bin:$PATH"
ENV EVALS_HTTP_ADDR=":8085"
ENV EVALS_KAFKA_BROKERS="kafka:29092"

EXPOSE 8085

ENTRYPOINT ["python", "-m", "tracewell_evals.main"]

