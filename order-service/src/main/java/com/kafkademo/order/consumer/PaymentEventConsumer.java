package com.kafkademo.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafkademo.order.model.PaymentProcessedEvent;
import com.kafkademo.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public PaymentEventConsumer(OrderService orderService) {
        this.orderService = orderService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @KafkaListener(
            topics = "payments.processed",
            groupId = "order-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentEvent(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received PaymentProcessedEvent from topic: {}, partition: {}, offset: {}",
                topic, partition, offset);
        log.debug("Payload: {}", payload);

        try {
            PaymentProcessedEvent event = objectMapper.readValue(payload, PaymentProcessedEvent.class);
            log.info("Processing PaymentProcessedEvent for orderId: {}, status: {}",
                    event.getOrderId(), event.getStatus());

            orderService.handlePaymentProcessed(event);
            acknowledgment.acknowledge();

            log.info("Successfully processed and acknowledged PaymentProcessedEvent for orderId: {}",
                    event.getOrderId());
        } catch (Exception e) {
            log.error("Error processing PaymentProcessedEvent from topic: {}, partition: {}, offset: {}. Payload: {}. Error: {}",
                    topic, partition, offset, payload, e.getMessage(), e);
            // Acknowledge to avoid reprocessing poison pills in this demo scenario.
            // In production, consider sending to a dead-letter topic instead.
            acknowledgment.acknowledge();
        }
    }
}
