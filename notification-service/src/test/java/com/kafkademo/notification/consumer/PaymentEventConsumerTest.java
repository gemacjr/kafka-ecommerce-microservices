package com.kafkademo.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkademo.notification.model.Notification;
import com.kafkademo.notification.model.PaymentProcessedEvent;
import com.kafkademo.notification.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for PaymentEventConsumer.
 *
 * Demonstrates consumer group isolation: notification-service-group consumes
 * payments.processed independently of order-service-group.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"payments.processed", "inventory.reserved"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
class PaymentEventConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NotificationService notificationService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // No-op: DB is reset per context via H2 create-drop; DirtiesContext handles isolation
    }

    @Test
    void whenPaymentSuccessEventPublished_thenPaymentSuccessNotificationCreated() throws Exception {
        // Arrange
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "order-001",
                "pay-001",
                "SUCCESS",
                null,
                LocalDateTime.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.send("payments.processed", "order-001", payload);

        // Wait for the consumer to process the message
        Thread.sleep(2000);

        // Assert
        List<Notification> notifications = notificationService.getRecentNotifications();
        assertThat(notifications).isNotEmpty();
        assertThat(notifications)
                .anyMatch(n -> "PAYMENT_SUCCESS".equals(n.getType())
                        && "order-001".equals(n.getOrderId()));
    }

    @Test
    void whenPaymentFailedEventPublished_thenPaymentFailedNotificationCreated() throws Exception {
        // Arrange
        PaymentProcessedEvent event = new PaymentProcessedEvent(
                "order-002",
                "pay-002",
                "FAILED",
                "Insufficient funds",
                LocalDateTime.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.send("payments.processed", "order-002", payload);

        // Wait for the consumer to process the message
        Thread.sleep(2000);

        // Assert
        List<Notification> notifications = notificationService.getRecentNotifications();
        assertThat(notifications).isNotEmpty();
        assertThat(notifications)
                .anyMatch(n -> "PAYMENT_FAILED".equals(n.getType())
                        && "order-002".equals(n.getOrderId())
                        && n.getMessage().contains("Insufficient funds"));
    }
}
