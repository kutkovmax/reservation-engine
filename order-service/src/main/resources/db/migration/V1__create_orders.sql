CREATE TABLE orders (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(100) NOT NULL UNIQUE,
    request_hash CHAR(64) NOT NULL,
    sku VARCHAR(64) NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    rejection_reason VARCHAR(40),
    aggregate_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    reserved_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    CONSTRAINT orders_idempotency_key_not_empty CHECK (length(idempotency_key) >= 1),
    CONSTRAINT orders_request_hash_format CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT orders_sku_format CHECK (sku ~ '^[A-Z0-9._-]{1,64}$'),
    CONSTRAINT orders_quantity_range CHECK (quantity BETWEEN 1 AND 1000000),
    CONSTRAINT orders_status_valid CHECK (
        status IN ('PENDING', 'RESERVED', 'REJECTED', 'CANCELLED', 'EXPIRED')
    ),
    CONSTRAINT orders_aggregate_version_positive CHECK (aggregate_version >= 1)
);
