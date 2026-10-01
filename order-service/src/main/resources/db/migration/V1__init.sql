CREATE TABLE orders (
    order_id          VARCHAR(36)    PRIMARY KEY,
    customer_id       VARCHAR(255)   NOT NULL,
    product_id        VARCHAR(255)   NOT NULL,
    quantity          INT            NOT NULL,
    total_amount      DECIMAL(19,2)  NOT NULL,
    status            VARCHAR(50)    NOT NULL,
    payment_confirmed BOOLEAN        NOT NULL DEFAULT FALSE,
    stock_confirmed   BOOLEAN        NOT NULL DEFAULT FALSE,
    placed_at         TIMESTAMP      NOT NULL
);
