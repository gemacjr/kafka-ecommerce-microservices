CREATE TABLE notifications (
    id       VARCHAR(36)  PRIMARY KEY,
    order_id VARCHAR(36)  NOT NULL,
    type     VARCHAR(100) NOT NULL,
    message  TEXT         NOT NULL,
    sent_at  TIMESTAMP    NOT NULL
);
