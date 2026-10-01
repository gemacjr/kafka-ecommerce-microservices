package com.kafkademo.notification.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(nullable = false, length = 100)
    private String type;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @Column(name = "sent_at", nullable = false)
    private LocalDateTime sentAt;

    public Notification() {
    }

    public Notification(String id, String orderId, String type, String message, LocalDateTime sentAt) {
        this.id = id;
        this.orderId = orderId;
        this.type = type;
        this.message = message;
        this.sentAt = sentAt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LocalDateTime getSentAt() {
        return sentAt;
    }

    public void setSentAt(LocalDateTime sentAt) {
        this.sentAt = sentAt;
    }

    @Override
    public String toString() {
        return "Notification{" +
                "id='" + id + '\'' +
                ", orderId='" + orderId + '\'' +
                ", type='" + type + '\'' +
                ", message='" + message + '\'' +
                ", sentAt=" + sentAt +
                '}';
    }
}
