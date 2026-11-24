-- Expand-only: V2 can continue using the database without reading this table.
CREATE TABLE IF NOT EXISTS worker_nodes (
    node_id         VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(120) NOT NULL,
    url             TEXT NOT NULL,
    use_h2c         BOOLEAN NOT NULL,
    weight          INTEGER NOT NULL CHECK (weight > 0),
    max_inflight    INTEGER NOT NULL CHECK (max_inflight >= 0),
    tags            JSONB NOT NULL DEFAULT '[]'::jsonb,
    note            TEXT NOT NULL DEFAULT '',
    enabled         BOOLEAN NOT NULL,
    secret_ref      VARCHAR(64) NOT NULL,
    drain_version   BIGINT NOT NULL DEFAULT 0 CHECK (drain_version >= 0),
    drain_entries   JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (NOT enabled OR drain_version = 0),
    CHECK (jsonb_typeof(tags) = 'array'),
    CHECK (jsonb_typeof(drain_entries) = 'array')
);
