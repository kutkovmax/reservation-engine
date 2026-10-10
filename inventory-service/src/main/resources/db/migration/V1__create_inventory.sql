CREATE TABLE inventory (
    sku VARCHAR(64) PRIMARY KEY,
    total_stock INTEGER NOT NULL,
    reserved_stock INTEGER NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inventory_total_stock_non_negative CHECK (total_stock >= 0),
    CONSTRAINT inventory_reserved_stock_valid CHECK (
        reserved_stock >= 0 AND reserved_stock <= total_stock
    )
);

INSERT INTO inventory (sku, total_stock)
VALUES
    ('BOOK-001', 100),
    ('LAPTOP-001', 10),
    ('PHONE-001', 25);
