CREATE TABLE payments (
    payment_id    VARCHAR(36)  PRIMARY KEY,
    order_id      VARCHAR(36)  UNIQUE NOT NULL,
    status        VARCHAR(20)  NOT NULL,
    failure_reason TEXT,
    processed_at  TIMESTAMP    NOT NULL
);
