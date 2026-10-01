package com.kafkademo.order.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderPlacedEvent {

    private String orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal totalAmount;
    private LocalDateTime placedAt;

    public OrderPlacedEvent() {
    }

    public OrderPlacedEvent(String orderId, String customerId, String productId,
                            int quantity, BigDecimal totalAmount, LocalDateTime placedAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.placedAt = placedAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public LocalDateTime getPlacedAt() {
        return placedAt;
    }

    public void setPlacedAt(LocalDateTime placedAt) {
        this.placedAt = placedAt;
    }

    @Override
    public String toString() {
        return "OrderPlacedEvent{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", totalAmount=" + totalAmount +
                ", placedAt=" + placedAt +
                '}';
    }
}
