package com.kafkademo.payment.model;

import java.time.LocalDateTime;

public class PaymentProcessedEvent {

    private String orderId;
    private String paymentId;
    private String status; // "SUCCESS" or "FAILED"
    private String failureReason;
    private LocalDateTime processedAt;

    public PaymentProcessedEvent() {
    }

    public PaymentProcessedEvent(String orderId, String paymentId, String status,
                                  String failureReason, LocalDateTime processedAt) {
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.status = status;
        this.failureReason = failureReason;
        this.processedAt = processedAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public String toString() {
        return "PaymentProcessedEvent{" +
                "orderId='" + orderId + '\'' +
                ", paymentId='" + paymentId + '\'' +
                ", status='" + status + '\'' +
                ", failureReason='" + failureReason + '\'' +
                ", processedAt=" + processedAt +
                '}';
    }
}
