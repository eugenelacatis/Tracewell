-- Hand-made rows for proj_dev so the dashboard has something to show before the writer runs.
-- Load from the repo root (safe to rerun; FINAL hides the duplicates):
--   docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell \
--     --multiquery < services/query/testdata/dev_seed.sql
INSERT INTO tracewell.traces (project_id, trace_id, session_id, user_id, name, tags, start_time, end_time, status, span_count, total_tokens, cost_usd, idempotency_key) VALUES
('proj_dev', 'd0000000000000000000000000000001', 'chat-42', 'user-7', 'chat turn', ['example'], '2026-10-26 17:04:05.000000', '2026-10-26 17:04:06.200000', 'OK', 3, 12, 0.0000045, 'seed:1'),
('proj_dev', 'd0000000000000000000000000000002', 'chat-42', 'user-7', 'chat turn', ['example'], '2026-10-26 17:05:10.000000', '2026-10-26 17:05:13.400000', 'ERROR', 2, 40, 0.000021, 'seed:1'),
('proj_dev', 'd0000000000000000000000000000003', 'batch-1', 'user-9', 'summarize doc', ['example', 'batch'], '2026-10-26 17:06:00.000000', '2026-10-26 17:06:02.500000', 'OK', 1, 820, 0.00041, 'seed:1');

INSERT INTO tracewell.spans (project_id, trace_id, span_id, parent_span_id, name, kind, start_time, end_time, status, error_message, provider, model, prompt, completion, params, prompt_tokens, completion_tokens, total_tokens, cost_usd, metadata) VALUES
('proj_dev', 'd0000000000000000000000000000001', '00f067aa0ba90001', '', 'handle message', 'SPAN', '2026-10-26 17:04:05.000000', '2026-10-26 17:04:06.200000', 'OK', '', '', '', '', '', '{}', 0, 0, 0, 0, '{"route":"/chat"}'),
('proj_dev', 'd0000000000000000000000000000001', '00f067aa0ba90002', '00f067aa0ba90001', 'retrieve context', 'SPAN', '2026-10-26 17:04:05.010000', '2026-10-26 17:04:05.090000', 'OK', '', '', '', '', '', '{}', 0, 0, 0, 0, '{"docs":3}'),
('proj_dev', 'd0000000000000000000000000000001', '00f067aa0ba90003', '00f067aa0ba90001', 'openai.chat', 'GENERATION', '2026-10-26 17:04:05.100000', '2026-10-26 17:04:06.100000', 'OK', '', 'openai', 'gpt-4o-mini', '[{"role":"user","content":"hi"}]', 'Hello!', '{"temperature":0.2}', 9, 3, 12, 0.0000045, '{}'),
('proj_dev', 'd0000000000000000000000000000002', '00f067aa0ba90004', '', 'handle message', 'SPAN', '2026-10-26 17:05:10.000000', '2026-10-26 17:05:13.400000', 'ERROR', 'upstream timeout', '', '', '', '', '{}', 0, 0, 0, 0, '{}'),
('proj_dev', 'd0000000000000000000000000000002', '00f067aa0ba90005', '00f067aa0ba90004', 'openai.chat', 'GENERATION', '2026-10-26 17:05:10.050000', '2026-10-26 17:05:13.350000', 'ERROR', 'timeout after 3300ms', 'openai', 'gpt-4o-mini', '[{"role":"user","content":"tell me a long story"}]', '', '{"temperature":0.9}', 40, 0, 40, 0.000021, '{}'),
('proj_dev', 'd0000000000000000000000000000003', '00f067aa0ba90006', '', 'anthropic.messages', 'GENERATION', '2026-10-26 17:06:00.000000', '2026-10-26 17:06:02.500000', 'OK', '', 'anthropic', 'claude-haiku-5-5', '[{"role":"user","content":"Summarize this document..."}]', 'The document describes...', '{"max_tokens":512}', 700, 120, 820, 0.00041, '{}');

INSERT INTO tracewell.evals (project_id, trace_id, span_id, evaluator, score, label, reasoning, created_at) VALUES
('proj_dev', 'd0000000000000000000000000000001', '00f067aa0ba90003', 'latency', 1.0, 'ok', '1000ms, under 5000ms', '2026-10-26 17:04:07.000000'),
('proj_dev', 'd0000000000000000000000000000001', '00f067aa0ba90003', 'empty_completion', 1.0, 'ok', 'completion is not empty', '2026-10-26 17:04:07.000000'),
('proj_dev', 'd0000000000000000000000000000002', '00f067aa0ba90005', 'latency', 1.0, 'ok', '3300ms, under 5000ms', '2026-10-26 17:05:14.000000'),
('proj_dev', 'd0000000000000000000000000000002', '00f067aa0ba90005', 'empty_completion', 0.0, 'empty', 'completion is empty', '2026-10-26 17:05:14.000000');
