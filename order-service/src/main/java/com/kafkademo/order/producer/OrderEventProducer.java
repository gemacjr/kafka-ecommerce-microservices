package com.kafkademo.order.producer;

import com.kafkademo.order.model.OrderCancelledEvent;
import com.kafkademo.order.model.OrderPlacedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private static final String ORDERS_PLACED_TOPIC = "orders.placed";
    private static final String ORDERS_CANCELLED_TOPIC = "orders.cancelled";

    // Custom header names — consumers can read these without deserialising the payload.
    // Useful for routing, filtering, and distributed tracing.
    static final String HEADER_CORRELATION_ID  = "X-Correlation-Id";
    static final String HEADER_EVENT_VERSION   = "X-Event-Version";
    static final String HEADER_SOURCE_SERVICE  = "X-Source-Service";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderPlaced(OrderPlacedEvent event) {
        log.info("Publishing OrderPlacedEvent for orderId: {}", event.getOrderId());

        // Use ProducerRecord directly (instead of the KafkaTemplate.send(topic, key, value)
        // shorthand) so we can attach custom headers.  Headers are byte arrays — convert
        // String values with UTF-8 encoding.
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                ORDERS_PLACED_TOPIC,
                null,                   // partition — null lets the partitioner choose by key
                event.getOrderId(),     // key — drives partition assignment for ordering
                event,
                java.util.List.of(
                        new RecordHeader(HEADER_CORRELATION_ID,
                                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(HEADER_EVENT_VERSION,
                                "1.0".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(HEADER_SOURCE_SERVICE,
                                "order-service".getBytes(StandardCharsets.UTF_8))
                )
        );

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("OrderPlacedEvent published successfully. OrderId: {}, Topic: {}, Partition: {}, Offset: {}",
                                event.getOrderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish OrderPlacedEvent for orderId: {}. Error: {}",
                                event.getOrderId(), ex.getMessage(), ex);
                    }
                });
    }

    public void publishOrderCancelled(OrderCancelledEvent event) {
        log.info("Publishing OrderCancelledEvent for orderId: {}", event.getOrderId());

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                ORDERS_CANCELLED_TOPIC,
                null,
                event.getOrderId(),
                event,
                java.util.List.of(
                        new RecordHeader(HEADER_CORRELATION_ID,
                                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(HEADER_EVENT_VERSION,
                                "1.0".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(HEADER_SOURCE_SERVICE,
                                "order-service".getBytes(StandardCharsets.UTF_8))
                )
        );

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("OrderCancelledEvent published successfully. OrderId: {}, Topic: {}, Partition: {}, Offset: {}",
                                event.getOrderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish OrderCancelledEvent for orderId: {}. Error: {}",
                                event.getOrderId(), ex.getMessage(), ex);
                    }
                });
    }
}
