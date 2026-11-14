CREATE TABLE IF NOT EXISTS prompts (
    id UUID PRIMARY KEY,
    text TEXT NOT NULL CHECK (char_length(btrim(text)) > 0 AND char_length(text) <= 8000),
    source TEXT NOT NULL DEFAULT 'web',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- P4 writes score/type/completion while decision stays pending; P7 finalizes the decision.
CREATE TABLE IF NOT EXISTS guard_results (
    prompt_id UUID PRIMARY KEY REFERENCES prompts(id) ON DELETE CASCADE,
    decision TEXT NOT NULL DEFAULT 'pending'
        CHECK (decision IN ('pending', 'allow', 'block', 'review')),
    malicious_score DOUBLE PRECISION CHECK (malicious_score BETWEEN 0 AND 1),
    attack_type TEXT,
    reason TEXT,
    model_version TEXT,
    checked_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dataset_prompts (
    -- Importers may pass a stable UUID (e.g. uuid5 of dataset/split/row) so re-imports skip existing rows.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    text TEXT NOT NULL CHECK (char_length(btrim(text)) > 0),
    label SMALLINT NOT NULL CHECK (label IN (0, 1)),
    source_dataset TEXT NOT NULL CHECK (source_dataset IN ('neuralchemy', 'deepset', 'jackhhao')),
    source TEXT,
    split TEXT NOT NULL,
    category TEXT,
    attack_group TEXT,
    group_id TEXT,
    include_in_baseline BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- One row per prompt the agent worker handled; 'rejected' means the stored decision was not 'allow'.
CREATE TABLE IF NOT EXISTS agent_runs (
    prompt_id UUID PRIMARY KEY REFERENCES prompts(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'rejected')),
    response TEXT,
    tool_calls JSONB NOT NULL DEFAULT '[]'::jsonb,
    latency_ms INTEGER,
    model TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Added after dataset_prompts exists; IF NOT EXISTS also upgrades databases created before this column.
-- NULL for prompts typed in the web UI; replays point to the dataset row they came from.
ALTER TABLE prompts ADD COLUMN IF NOT EXISTS dataset_prompt_id UUID
    REFERENCES dataset_prompts(id) ON DELETE SET NULL;

-- P10 rate-limiter output: one row per prompt seen by the burst worker (allow or block).
-- Thresholds are stored on every row so experiments are reproducible and usable for learning.
CREATE TABLE IF NOT EXISTS throttle_decisions (
    prompt_id UUID PRIMARY KEY REFERENCES prompts(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('allow', 'block')),
    reason TEXT,                    -- 'burst_detected' | 'source_blocked' | NULL for allow
    window_count INTEGER NOT NULL CHECK (window_count >= 0),  -- prompts from this source in the window, incl. this one
    window_seconds INTEGER NOT NULL CHECK (window_seconds > 0),
    threshold INTEGER NOT NULL CHECK (threshold > 0),
    blocked_until TIMESTAMPTZ,      -- set while the source is blocked
    burst_started BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE only on the prompt that started the block
    event_time TIMESTAMPTZ NOT NULL,  -- the raw event's created_at
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- P7 transactional outbox: an allowed prompt is retried until Kafka acknowledges it.
CREATE TABLE IF NOT EXISTS allowed_prompt_outbox (
    prompt_id UUID PRIMARY KEY REFERENCES prompts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

-- P8's single current view and timestamped rolling-window history.
CREATE TABLE IF NOT EXISTS attack_aggregate_current (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    snapshot_at TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    attack_type_counts JSONB NOT NULL,
    top_patterns JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS attack_aggregate_history (
    snapshot_id BIGSERIAL PRIMARY KEY,
    snapshot_at TIMESTAMPTZ NOT NULL UNIQUE,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    attack_type_counts JSONB NOT NULL,
    top_patterns JSONB NOT NULL
);

-- P1/P2 snapshot; older prompts are filled only if their event is delivered again.
ALTER TABLE prompts ADD COLUMN IF NOT EXISTS preprocessing JSONB;

-- P4 completion survives P7 replacing checked_at with the final routing time.
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;
-- P3 similarity is retained so P5 can recover idempotently after a restart.
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS p3_similarity_score DOUBLE PRECISION
    CHECK (p3_similarity_score BETWEEN -1 AND 1);
-- P5 anomaly output and feature version support at-least-once Kafka delivery and rebuilds.
CREATE SEQUENCE IF NOT EXISTS p5_event_sequence;
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS anomaly_score DOUBLE PRECISION
    CHECK (anomaly_score >= 0);
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS p5_status TEXT;
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS p5_feature_version TEXT;
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS p5_processed_at TIMESTAMPTZ;
ALTER TABLE guard_results ADD COLUMN IF NOT EXISTS p5_sequence BIGINT;

CREATE INDEX IF NOT EXISTS prompts_created_at_idx ON prompts (created_at DESC);
CREATE INDEX IF NOT EXISTS throttle_decisions_source_time_idx ON throttle_decisions (source, event_time DESC);
CREATE INDEX IF NOT EXISTS throttle_decisions_bursts_idx ON throttle_decisions (event_time DESC) WHERE burst_started;
CREATE INDEX IF NOT EXISTS throttle_decisions_allow_decided_idx ON throttle_decisions (decided_at, prompt_id) WHERE action = 'allow';
CREATE INDEX IF NOT EXISTS allowed_prompt_outbox_pending_idx ON allowed_prompt_outbox (created_at) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS attack_aggregate_history_snapshot_idx ON attack_aggregate_history (snapshot_at DESC);
CREATE INDEX IF NOT EXISTS prompts_dataset_prompt_id_idx ON prompts (dataset_prompt_id);
CREATE INDEX IF NOT EXISTS prompts_source_idx ON prompts (source);
CREATE INDEX IF NOT EXISTS dataset_prompts_source_split_idx ON dataset_prompts (source_dataset, split);
CREATE INDEX IF NOT EXISTS guard_results_p5_rebuild_idx
    ON guard_results (p5_feature_version, p5_sequence DESC) WHERE p5_sequence IS NOT NULL;
