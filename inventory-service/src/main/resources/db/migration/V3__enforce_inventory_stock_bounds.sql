UPDATE inventory
SET reserved_stock = total_stock,
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE reserved_stock > total_stock;

ALTER TABLE inventory
    DROP CONSTRAINT inventory_reserved_stock_non_negative;

ALTER TABLE inventory
    ADD CONSTRAINT inventory_reserved_stock_valid CHECK (
        reserved_stock >= 0 AND reserved_stock <= total_stock
    );
