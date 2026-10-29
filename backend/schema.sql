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

-- Added after dataset_prompts exists; IF NOT EXISTS also upgrades databases created before this column.
-- NULL for prompts typed in the web UI; replays point to the dataset row they came from.
ALTER TABLE prompts ADD COLUMN IF NOT EXISTS dataset_prompt_id UUID
    REFERENCES dataset_prompts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS prompts_created_at_idx ON prompts (created_at DESC);
CREATE INDEX IF NOT EXISTS prompts_dataset_prompt_id_idx ON prompts (dataset_prompt_id);
CREATE INDEX IF NOT EXISTS dataset_prompts_source_split_idx ON dataset_prompts (source_dataset, split);
