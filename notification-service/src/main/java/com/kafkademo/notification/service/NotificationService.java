package com.kafkademo.notification.service;

import com.kafkademo.notification.model.Notification;
import com.kafkademo.notification.model.PaymentProcessedEvent;
import com.kafkademo.notification.model.StockReservedEvent;
import com.kafkademo.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public void sendPaymentNotification(PaymentProcessedEvent event) {
        String type;
        String message;
        if ("SUCCESS".equals(event.getStatus())) {
            type = "PAYMENT_SUCCESS";
            message = "Payment confirmed for order " + event.getOrderId();
        } else {
            type = "PAYMENT_FAILED";
            message = "Payment failed for order " + event.getOrderId()
                    + (event.getFailureReason() != null ? ": " + event.getFailureReason() : "");
        }
        Notification notification = new Notification(UUID.randomUUID().toString(),
                event.getOrderId(), type, message, LocalDateTime.now());
        notificationRepository.save(notification);
        log.info("[NOTIFICATION] {} — orderId={}, message={}", type, event.getOrderId(), message);
    }

    public void sendStockNotification(StockReservedEvent event) {
        String type;
        String message;
        if ("RESERVED".equals(event.getStatus())) {
            type = "STOCK_RESERVED";
            message = "Stock reserved for order " + event.getOrderId()
                    + ", product " + event.getProductId();
        } else {
            type = "STOCK_INSUFFICIENT";
            message = "Insufficient stock for order " + event.getOrderId()
                    + ", available: " + event.getAvailableStock();
        }
        Notification notification = new Notification(UUID.randomUUID().toString(),
                event.getOrderId(), type, message, LocalDateTime.now());
        notificationRepository.save(notification);
        log.info("[NOTIFICATION] {} — orderId={}, message={}", type, event.getOrderId(), message);
    }

    public List<Notification> getRecentNotifications() {
        return notificationRepository.findAll();
    }
}
