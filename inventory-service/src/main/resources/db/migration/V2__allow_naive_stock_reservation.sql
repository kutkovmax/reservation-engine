ALTER TABLE inventory
    DROP CONSTRAINT inventory_reserved_stock_valid;

ALTER TABLE inventory
    ADD CONSTRAINT inventory_reserved_stock_non_negative
        CHECK (reserved_stock >= 0);
