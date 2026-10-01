package com.kafkademo.notification.service;

import com.kafkademo.notification.model.Notification;
import com.kafkademo.notification.model.PaymentProcessedEvent;
import com.kafkademo.notification.model.StockReservedEvent;
import com.kafkademo.notification.repository.NotificationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure unit tests for NotificationService business logic.
 * No Kafka infrastructure required — tests only the notification creation rules.
 */
@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    private NotificationRepository notificationRepository;

    @InjectMocks
    private NotificationService notificationService;

    // --- Payment notification tests ---

    @Test
    void sendPaymentNotification_withSuccessStatus_createsPaymentSuccessNotification() {
        // Arrange
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "order-001",
                "pay-001",
                "SUCCESS",
                null,
                LocalDateTime.now()
        );
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        when(notificationRepository.save(any(Notification.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        // Act
        notificationService.sendPaymentNotification(event);

        // Assert
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getType()).isEqualTo("PAYMENT_SUCCESS");
        assertThat(notification.getOrderId()).isEqualTo("order-001");
        assertThat(notification.getMessage()).contains("order-001");
        assertThat(notification.getId()).isNotBlank();
        assertThat(notification.getSentAt()).isNotNull();
    }

    @Test
    void sendPaymentNotification_withFailedStatus_createsPaymentFailedNotification() {
        // Arrange
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "order-002",
                "pay-002",
                "FAILED",
                "Card declined",
                LocalDateTime.now()
        );
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        when(notificationRepository.save(any(Notification.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        // Act
        notificationService.sendPaymentNotification(event);

        // Assert
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getType()).isEqualTo("PAYMENT_FAILED");
        assertThat(notification.getOrderId()).isEqualTo("order-002");
        assertThat(notification.getMessage()).contains("order-002");
        assertThat(notification.getMessage()).contains("Card declined");
    }

    // --- Stock notification tests ---

    @Test
    void sendStockNotification_withReservedStatus_createsStockReservedNotification() {
        // Arrange
        StockReservedEvent event = new StockReservedEvent(
                "order-101",
                "prod-abc",
                "RESERVED",
                100,
                LocalDateTime.now()
        );
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        when(notificationRepository.save(any(Notification.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        // Act
        notificationService.sendStockNotification(event);

        // Assert
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getType()).isEqualTo("STOCK_RESERVED");
        assertThat(notification.getOrderId()).isEqualTo("order-101");
        assertThat(notification.getMessage()).contains("order-101");
        assertThat(notification.getMessage()).contains("prod-abc");
    }

    @Test
    void sendStockNotification_withInsufficientStatus_createsStockInsufficientNotification() {
        // Arrange
        StockReservedEvent event = new StockReservedEvent(
                "order-102",
                "prod-xyz",
                "INSUFFICIENT",
                3,
                LocalDateTime.now()
        );
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        when(notificationRepository.save(any(Notification.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        // Act
        notificationService.sendStockNotification(event);

        // Assert
        verify(notificationRepository).save(captor.capture());
        Notification notification = captor.getValue();
        assertThat(notification.getType()).isEqualTo("STOCK_INSUFFICIENT");
        assertThat(notification.getOrderId()).isEqualTo("order-102");
        assertThat(notification.getMessage()).contains("available: 3");
    }

    // --- getRecentNotifications tests ---

    @Test
    void getRecentNotifications_returnsAllNotificationsAdded() {
        // Arrange — stub three saved notifications returned by findAll
        Notification n1 = new Notification(
                "id-1", "order-A", "PAYMENT_SUCCESS", "Payment confirmed for order order-A", LocalDateTime.now());
        Notification n2 = new Notification(
                "id-2", "order-B", "PAYMENT_FAILED", "Payment failed for order order-B: Expired card", LocalDateTime.now());
        Notification n3 = new Notification(
                "id-3", "order-C", "STOCK_RESERVED", "Stock reserved for order order-C, product prod-C", LocalDateTime.now());
        when(notificationRepository.findAll()).thenReturn(List.of(n1, n2, n3));

        // Act
        List<Notification> notifications = notificationService.getRecentNotifications();

        // Assert
        assertThat(notifications).hasSize(3);
        assertThat(notifications).extracting(Notification::getType)
                .containsExactlyInAnyOrder("PAYMENT_SUCCESS", "PAYMENT_FAILED", "STOCK_RESERVED");
    }
}
