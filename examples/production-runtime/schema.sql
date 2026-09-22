CREATE TABLE IF NOT EXISTS id_generator_node_leases (
    node_id INTEGER PRIMARY KEY CHECK (node_id >= 1 AND node_id <= 1024),
    owner_id TEXT NOT NULL,
    reserved_until_ms BIGINT NOT NULL CHECK (reserved_until_ms > 0),
    generation_fence_ms BIGINT NOT NULL CHECK (generation_fence_ms > 0),
    acquired_at_ms BIGINT NOT NULL CHECK (acquired_at_ms > 0),
    refreshed_at_ms BIGINT NOT NULL CHECK (refreshed_at_ms > 0),
    heartbeat_at_ms BIGINT NOT NULL CHECK (heartbeat_at_ms > 0),
    lease_version BIGINT NOT NULL DEFAULT 1 CHECK (lease_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_id_generator_node_leases_reserved_until
    ON id_generator_node_leases (reserved_until_ms);

-- generation_fence_ms is monotonic by contract. Do not shorten or delete this
-- row during normal deploys; an expired owner still has to wait for the fence.
