CREATE TABLE id_generator_node_leases (
    node_id INTEGER PRIMARY KEY,
    owner_id TEXT NOT NULL,
    reserved_until_ms BIGINT NOT NULL CHECK (reserved_until_ms > 0),
    generation_fence_ms BIGINT NOT NULL CHECK (generation_fence_ms > 0),
    acquired_at_ms BIGINT NOT NULL,
    refreshed_at_ms BIGINT NOT NULL,
    heartbeat_at_ms BIGINT NOT NULL,
    lease_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
