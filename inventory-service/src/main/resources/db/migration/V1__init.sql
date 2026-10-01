CREATE TABLE stock_items (
    product_id      VARCHAR(255) PRIMARY KEY,
    available_stock INT          NOT NULL CHECK (available_stock >= 0)
);

CREATE TABLE reservations (
    order_id   VARCHAR(36)  PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    quantity   INT          NOT NULL
);

-- Seed data (replaces @PostConstruct initialization)
INSERT INTO stock_items (product_id, available_stock) VALUES
    ('prod-1', 100),
    ('prod-2', 5),
    ('prod-3', 50);
