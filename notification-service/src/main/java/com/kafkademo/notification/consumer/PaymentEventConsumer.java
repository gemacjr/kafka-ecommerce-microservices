package com.kafkademo.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.notification.model.PaymentProcessedEvent;
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
 * Consumer for payment events from the payments.processed topic.
 *
 * IMPORTANT DEMO CONCEPT — Consumer Group Isolation:
 * This consumer group (notification-service-group) is completely independent from
 * order-service-group. Both groups receive every message on payments.processed.
 * This is the power of Kafka consumer groups — multiple subscribers to the same
 * topic, each maintaining their own offset and processing messages independently.
 * Notification-service receiving this event does NOT affect what order-service sees.
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public PaymentEventConsumer(NotificationService notificationService, ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "payments.processed",
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentProcessed(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.debug("Notification-service received payment event from partition={}, offset={}", partition, offset);

        try {
            PaymentProcessedEvent event = objectMapper.readValue(payload, PaymentProcessedEvent.class);
            log.info("Processing PaymentProcessedEvent: orderId={}, status={}", event.getOrderId(), event.getStatus());

            notificationService.sendPaymentNotification(event);

            acknowledgment.acknowledge();
            log.debug("Acknowledged payment event at partition={}, offset={}", partition, offset);

        } catch (Exception e) {
            log.error("Failed to process payment event from topic={}, partition={}, offset={}: {}",
                    topic, partition, offset, e.getMessage(), e);
            // Acknowledge to avoid infinite reprocessing of a poison pill message.
            // In production, route to a DLQ instead.
            acknowledgment.acknowledge();
        }
    }
}
