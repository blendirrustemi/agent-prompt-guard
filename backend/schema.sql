CREATE TABLE IF NOT EXISTS prompts (
    id UUID PRIMARY KEY,
    text TEXT NOT NULL CHECK (char_length(btrim(text)) > 0 AND char_length(text) <= 8000),
    source TEXT NOT NULL DEFAULT 'web',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE INDEX IF NOT EXISTS prompts_created_at_idx ON prompts (created_at DESC);
