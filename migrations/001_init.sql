CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    balance     BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS requests (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT NOT NULL REFERENCES users(id),
    idempotency_key  TEXT NOT NULL,
    payload_hash     TEXT NOT NULL,
    total_cost       BIGINT NOT NULL DEFAULT 0,
    accepted_count   INT NOT NULL DEFAULT 0,
    rejected_count   INT NOT NULL DEFAULT 0,
    response_json    JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS messages (
    id            BIGSERIAL PRIMARY KEY,
    request_id    BIGINT NOT NULL REFERENCES requests(id),
    user_id       BIGINT NOT NULL REFERENCES users(id),
    recipient     TEXT NOT NULL,
    type          TEXT NOT NULL CHECK (type IN ('otp', 'text')),
    delivery_mode TEXT NOT NULL CHECK (delivery_mode IN ('express', 'normal')),
    text          TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('accepted', 'queued', 'sending', 'sent', 'failed', 'rejected')),
    cost          BIGINT NOT NULL DEFAULT 0,
    error_code    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  BIGINT NOT NULL,
    topic         TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload       JSONB NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('pending', 'publishing', 'published', 'failed')),
    attempts      INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ,
    locked_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_messages_request_id ON messages(request_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status);
