package com.kafkademo.inventory.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.inventory.model.OrderCancelledEvent;
import com.kafkademo.inventory.model.OrderPlacedEvent;
import com.kafkademo.inventory.service.InventoryManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final InventoryManagementService inventoryManagementService;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(InventoryManagementService inventoryManagementService,
                              ObjectMapper objectMapper) {
        this.inventoryManagementService = inventoryManagementService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "orders.placed",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderPlaced(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("Received OrderPlacedEvent | topic={}, partition={}, offset={}", topic, partition, offset);
        log.debug("OrderPlacedEvent payload: {}", payload);

        try {
            OrderPlacedEvent event = objectMapper.readValue(payload, OrderPlacedEvent.class);
            inventoryManagementService.reserveStock(event);
            acknowledgment.acknowledge();
            log.info("Successfully processed OrderPlacedEvent for orderId={}", event.getOrderId());
        } catch (Exception e) {
            log.error("Error processing OrderPlacedEvent | topic={}, partition={}, offset={}: {}",
                    topic, partition, offset, e.getMessage(), e);
            // Acknowledge to avoid poison-pill loop in a learning project.
            // In production, consider a dead-letter topic strategy instead.
            acknowledgment.acknowledge();
        }
    }

    @KafkaListener(
            topics = "orders.cancelled",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCancelled(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("Received OrderCancelledEvent | topic={}, partition={}, offset={}", topic, partition, offset);
        log.debug("OrderCancelledEvent payload: {}", payload);

        try {
            OrderCancelledEvent event = objectMapper.readValue(payload, OrderCancelledEvent.class);
            inventoryManagementService.releaseStock(event);
            acknowledgment.acknowledge();
            log.info("Successfully processed OrderCancelledEvent for orderId={}", event.getOrderId());
        } catch (Exception e) {
            log.error("Error processing OrderCancelledEvent | topic={}, partition={}, offset={}: {}",
                    topic, partition, offset, e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
}
