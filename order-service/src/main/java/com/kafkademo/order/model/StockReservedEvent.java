package com.kafkademo.order.model;

import java.time.LocalDateTime;

public class StockReservedEvent {

    private String orderId;
    private String productId;
    private String status; // "RESERVED" or "INSUFFICIENT"
    private int availableStock;
    private LocalDateTime reservedAt;

    public StockReservedEvent() {
    }

    public StockReservedEvent(String orderId, String productId, String status,
                              int availableStock, LocalDateTime reservedAt) {
        this.orderId = orderId;
        this.productId = productId;
        this.status = status;
        this.availableStock = availableStock;
        this.reservedAt = reservedAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getAvailableStock() {
        return availableStock;
    }

    public void setAvailableStock(int availableStock) {
        this.availableStock = availableStock;
    }

    public LocalDateTime getReservedAt() {
        return reservedAt;
    }

    public void setReservedAt(LocalDateTime reservedAt) {
        this.reservedAt = reservedAt;
    }

    @Override
    public String toString() {
        return "StockReservedEvent{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", status='" + status + '\'' +
                ", availableStock=" + availableStock +
                ", reservedAt=" + reservedAt +
                '}';
    }
}
