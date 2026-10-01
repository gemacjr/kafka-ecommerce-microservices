package com.kafkademo.inventory.model;

import java.time.LocalDateTime;

public class OrderCancelledEvent {

    private String orderId;
    private LocalDateTime cancelledAt;

    public OrderCancelledEvent() {
    }

    public OrderCancelledEvent(String orderId, LocalDateTime cancelledAt) {
        this.orderId = orderId;
        this.cancelledAt = cancelledAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public LocalDateTime getCancelledAt() {
        return cancelledAt;
    }

    public void setCancelledAt(LocalDateTime cancelledAt) {
        this.cancelledAt = cancelledAt;
    }

    @Override
    public String toString() {
        return "OrderCancelledEvent{" +
                "orderId='" + orderId + '\'' +
                ", cancelledAt=" + cancelledAt +
                '}';
    }
}
