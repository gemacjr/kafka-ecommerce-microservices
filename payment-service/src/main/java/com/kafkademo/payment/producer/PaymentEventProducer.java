package com.kafkademo.payment.producer;

import com.kafkademo.payment.model.PaymentProcessedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String PAYMENTS_PROCESSED_TOPIC = "payments.processed";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentProcessed(PaymentProcessedEvent event) {
        log.info("Publishing PaymentProcessedEvent for orderId={} status={}",
                event.getOrderId(), event.getStatus());

        kafkaTemplate.send(PAYMENTS_PROCESSED_TOPIC, event.getOrderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("PaymentProcessedEvent published successfully: orderId={}, topic={}, partition={}, offset={}",
                                event.getOrderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish PaymentProcessedEvent for orderId={}: {}",
                                event.getOrderId(), ex.getMessage(), ex);
                    }
                });
    }
}
