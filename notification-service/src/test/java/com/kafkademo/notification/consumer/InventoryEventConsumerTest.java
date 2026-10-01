package com.kafkademo.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkademo.notification.model.Notification;
import com.kafkademo.notification.model.StockReservedEvent;
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
 * Integration test for InventoryEventConsumer.
 *
 * Verifies that stock reservation events from inventory.reserved are consumed
 * and trigger the correct notification type in notification-service-group.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"payments.processed", "inventory.reserved"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
class InventoryEventConsumerTest {

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
    void whenStockReservedEventPublished_thenStockReservedNotificationCreated() throws Exception {
        // Arrange
        StockReservedEvent event = new StockReservedEvent(
                "order-101",
                "prod-abc",
                "RESERVED",
                50,
                LocalDateTime.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.send("inventory.reserved", "order-101", payload);

        // Wait for the consumer to process the message
        Thread.sleep(2000);

        // Assert
        List<Notification> notifications = notificationService.getRecentNotifications();
        assertThat(notifications).isNotEmpty();
        assertThat(notifications)
                .anyMatch(n -> "STOCK_RESERVED".equals(n.getType())
                        && "order-101".equals(n.getOrderId())
                        && n.getMessage().contains("prod-abc"));
    }

    @Test
    void whenStockInsufficientEventPublished_thenStockInsufficientNotificationCreated() throws Exception {
        // Arrange
        StockReservedEvent event = new StockReservedEvent(
                "order-102",
                "prod-xyz",
                "INSUFFICIENT",
                2,
                LocalDateTime.now()
        );
        String payload = objectMapper.writeValueAsString(event);

        // Act
        kafkaTemplate.send("inventory.reserved", "order-102", payload);

        // Wait for the consumer to process the message
        Thread.sleep(2000);

        // Assert
        List<Notification> notifications = notificationService.getRecentNotifications();
        assertThat(notifications).isNotEmpty();
        assertThat(notifications)
                .anyMatch(n -> "STOCK_INSUFFICIENT".equals(n.getType())
                        && "order-102".equals(n.getOrderId())
                        && n.getMessage().contains("available: 2"));
    }
}
