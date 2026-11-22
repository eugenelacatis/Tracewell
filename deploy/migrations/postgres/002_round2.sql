-- Tracewell Postgres schema, round 2 additions. Runs after postgres.sql and is safe to run twice.
-- Contract owned by all tracks. Track B copies this into deploy/migrations/postgres/002_round2.sql
-- unchanged, so existing volumes pick it up without a reset.
-- Readers/writers: query-svc (users, memberships, alert_rules CRUD), alerter (alert_rules).

-- Dashboard users. Login is email + password; the session lives in Redis (see README.md, Redis keys).
CREATE TABLE IF NOT EXISTS users (
    id            text PRIMARY KEY,               -- e.g. 'usr_dev'
    email         text NOT NULL UNIQUE,           -- stored lowercased
    name          text NOT NULL,
    password_hash text NOT NULL,                  -- bcrypt, cost 12
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- A user sees every project in the orgs they belong to, and nothing else.
CREATE TABLE IF NOT EXISTS memberships (
    user_id    text NOT NULL REFERENCES users (id),
    org_id     text NOT NULL REFERENCES orgs (id),
    role       text NOT NULL CHECK (role IN ('owner', 'member')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, org_id)
);

-- alert_events references alert_rules, so rules are soft-deleted. The alerter loads only
-- rows WHERE enabled AND deleted_at IS NULL. The dashboard never lists deleted rules.
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- Local dev seed.
-- Dashboard login: dev@tracewell.local / tracewell (member of org_dev only).
-- A second org and project prove tenant isolation: the dev user must never see proj_demo.
-- proj_demo API key: tw_demo_otherkey
INSERT INTO users (id, email, name, password_hash)
VALUES ('usr_dev', 'dev@tracewell.local', 'Dev User',
        '$2a$12$foK1N.6gae1SAK/VpUMbH.1vbxL1Wv6g2JMT5YMsq8Sg/JdBovZES')
ON CONFLICT DO NOTHING;
INSERT INTO memberships (user_id, org_id, role) VALUES ('usr_dev', 'org_dev', 'owner') ON CONFLICT DO NOTHING;

INSERT INTO orgs (id, name) VALUES ('org_demo', 'Other Org') ON CONFLICT DO NOTHING;
INSERT INTO projects (id, org_id, name) VALUES ('proj_demo', 'org_demo', 'Other Project') ON CONFLICT DO NOTHING;
INSERT INTO api_keys (project_id, name, key_prefix, key_hash)
VALUES ('proj_demo', 'demo', 'tw_demo_ot',
        '2deff781a694fbee2819f7fe34ec9ac25f5e25514cbeee11ed4ed973acaae1f9')
ON CONFLICT DO NOTHING;
