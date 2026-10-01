package com.kafkademo.order.model;

public enum OrderStatus {
    PENDING,
    PAYMENT_CONFIRMED,
    PAYMENT_FAILED,
    STOCK_CONFIRMED,
    STOCK_INSUFFICIENT,
    CONFIRMED,
    CANCELLED
}
