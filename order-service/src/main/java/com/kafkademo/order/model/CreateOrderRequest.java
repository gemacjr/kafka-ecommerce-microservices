package com.kafkademo.order.model;

import java.math.BigDecimal;

public class CreateOrderRequest {

    private String orderId; // optional — if provided, order-service uses it; otherwise generates UUID
    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal totalAmount;

    public CreateOrderRequest() {
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

    @Override
    public String toString() {
        return "CreateOrderRequest{" +
                "customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
