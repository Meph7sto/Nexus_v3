CREATE TABLE IF NOT EXISTS entry_shared_task_claim (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    entry_id TEXT NOT NULL,
    process_token TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
