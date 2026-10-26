-- Tracewell Postgres schema, round 1 (metadata only; traces live in ClickHouse).
-- Contract owned by all tracks. Track B copies this into deploy/migrations/postgres/ unchanged.
-- Readers: ingest-svc and query-svc (api_keys), alerter (alert_rules, alert_events).
-- Round 2 adds users and memberships for dashboard login.

CREATE TABLE IF NOT EXISTS orgs (
    id         text PRIMARY KEY,                 -- e.g. 'org_dev'
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS projects (
    id         text PRIMARY KEY,                 -- e.g. 'proj_dev'; this is project_id everywhere
    org_id     text NOT NULL REFERENCES orgs (id),
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- A key is checked by hashing the presented key and looking it up:
--   key_hash = lowercase hex sha256 of the full key string (UTF-8)
--   valid when a row matches and revoked_at IS NULL
-- The plaintext key is never stored.
CREATE TABLE IF NOT EXISTS api_keys (
    id         bigserial PRIMARY KEY,
    project_id text NOT NULL REFERENCES projects (id),
    name       text NOT NULL,
    key_prefix text NOT NULL,                    -- first 10 chars, for display only
    key_hash   text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

CREATE TABLE IF NOT EXISTS alert_rules (
    id               bigserial PRIMARY KEY,
    project_id       text NOT NULL REFERENCES projects (id),
    name             text NOT NULL,
    metric           text NOT NULL CHECK (metric IN ('error_rate', 'cost_usd', 'p95_latency_ms')),
    threshold        double precision NOT NULL,  -- fires when metric > threshold
    window_seconds   integer NOT NULL DEFAULT 60,
    cooldown_seconds integer NOT NULL DEFAULT 300,
    channel          text NOT NULL CHECK (channel IN ('log', 'webhook', 'slack')),
    target           text NOT NULL DEFAULT '',   -- URL for webhook/slack, '' for log
    enabled          boolean NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Written by the alerter each time a rule fires. Read by the dashboard.
CREATE TABLE IF NOT EXISTS alert_events (
    id         bigserial PRIMARY KEY,
    rule_id    bigint NOT NULL REFERENCES alert_rules (id),
    project_id text NOT NULL REFERENCES projects (id),
    value      double precision NOT NULL,        -- the metric value that crossed the threshold
    message    text NOT NULL,
    fired_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_events_project_fired ON alert_events (project_id, fired_at DESC);

-- Local dev seed. The dev API key is: tw_dev_localkey
INSERT INTO orgs (id, name) VALUES ('org_dev', 'Dev Org') ON CONFLICT DO NOTHING;
INSERT INTO projects (id, org_id, name) VALUES ('proj_dev', 'org_dev', 'Dev Project') ON CONFLICT DO NOTHING;
INSERT INTO api_keys (project_id, name, key_prefix, key_hash)
VALUES ('proj_dev', 'local dev', 'tw_dev_loc',
        '1cb9ec7c756e05df0214a49a974a0931046e9e7e317ec0160541b08c07bf1dea')
ON CONFLICT DO NOTHING;
INSERT INTO alert_rules (project_id, name, metric, threshold, window_seconds, cooldown_seconds, channel)
SELECT 'proj_dev', 'error rate above 50%', 'error_rate', 0.5, 60, 60, 'log'
WHERE NOT EXISTS (SELECT 1 FROM alert_rules WHERE project_id = 'proj_dev');
