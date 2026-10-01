package com.kafkademo.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.payment.model.OrderPlacedEvent;
import com.kafkademo.payment.service.PaymentProcessingService;
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

    private final PaymentProcessingService paymentProcessingService;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(PaymentProcessingService paymentProcessingService,
                               ObjectMapper objectMapper) {
        this.paymentProcessingService = paymentProcessingService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "orders.placed",
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderPlaced(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            // Custom headers published by order-service/OrderEventProducer.
            // required=false keeps the consumer backward-compatible: if the header
            // is absent (e.g., messages produced before this change) the parameter
            // is simply null instead of throwing a MissingHeaderException.
            @Header(value = "X-Correlation-Id",  required = false) String correlationId,
            @Header(value = "X-Event-Version",   required = false) String eventVersion,
            @Header(value = "X-Source-Service",  required = false) String sourceService,
            Acknowledgment acknowledgment) {

        log.info("Received message from topic={}, partition={}, offset={} " +
                 "[correlationId={}, eventVersion={}, sourceService={}]",
                topic, partition, offset, correlationId, eventVersion, sourceService);
        log.debug("Payload: {}", payload);

        try {
            OrderPlacedEvent event = objectMapper.readValue(payload, OrderPlacedEvent.class);
            log.info("Parsed OrderPlacedEvent: orderId={}, customerId={}, totalAmount={}",
                    event.getOrderId(), event.getCustomerId(), event.getTotalAmount());

            paymentProcessingService.processPayment(event);

            // Only acknowledge after successful processing
            acknowledgment.acknowledge();
            log.info("Message acknowledged for orderId={}", event.getOrderId());

        } catch (Exception ex) {
            log.error("Error processing message from topic={}, partition={}, offset={}: {}",
                    topic, partition, offset, ex.getMessage(), ex);
            // Do NOT acknowledge — re-throw so DefaultErrorHandler can retry / route to DLQ
            throw new RuntimeException("Failed to process order event", ex);
        }
    }
}
