package com.kafkademo.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.notification.model.StockReservedEvent;
import com.kafkademo.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer for inventory events from the inventory.reserved topic.
 *
 * Uses the same notification-service-group consumer group as PaymentEventConsumer,
 * so all notification-related event consumption shares a single group identity
 * for offset tracking and load balancing purposes.
 */
@Component
public class InventoryEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventConsumer.class);

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public InventoryEventConsumer(NotificationService notificationService, ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "inventory.reserved",
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeStockReserved(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.debug("Notification-service received inventory event from partition={}, offset={}", partition, offset);

        try {
            StockReservedEvent event = objectMapper.readValue(payload, StockReservedEvent.class);
            log.info("Processing StockReservedEvent: orderId={}, productId={}, status={}",
                    event.getOrderId(), event.getProductId(), event.getStatus());

            notificationService.sendStockNotification(event);

            acknowledgment.acknowledge();
            log.debug("Acknowledged inventory event at partition={}, offset={}", partition, offset);

        } catch (Exception e) {
            log.error("Failed to process inventory event from topic={}, partition={}, offset={}: {}",
                    topic, partition, offset, e.getMessage(), e);
            // Acknowledge to avoid infinite reprocessing of a poison pill message.
            // In production, route to a DLQ instead.
            acknowledgment.acknowledge();
        }
    }
}
