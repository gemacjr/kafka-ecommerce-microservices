package com.kafkademo.payment.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.payment.model.OrderPlacedEvent;
import com.kafkademo.payment.service.PaymentProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test demonstrating the DLQ (Dead Letter Queue) pattern.
 *
 * Flow:
 *  1. Register orderId for forced failure via PaymentProcessingService.forceFail()
 *  2. Send an OrderPlacedEvent to orders.placed
 *  3. The consumer processes it → PaymentProcessingException is thrown immediately (non-retryable)
 *  4. DefaultErrorHandler routes the original record to payments.dlq
 *  5. We assert the message arrived on payments.dlq
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"orders.placed", "payments.processed", "payments.dlq"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext
class DlqIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private PaymentProcessingService paymentProcessingService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaConsumer<String, String> dlqConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "dlq-test-group-" + UUID.randomUUID(),
                "true",
                embeddedKafkaBroker
        );
        dlqConsumer = new KafkaConsumer<>(consumerProps);
        dlqConsumer.subscribe(Collections.singletonList("payments.dlq"));
    }

    @AfterEach
    void tearDown() {
        if (dlqConsumer != null) {
            dlqConsumer.close();
        }
    }

    @Test
    void whenPaymentProcessingExceptionThrown_thenMessageIsRoutedToDlq() throws Exception {
        // 1. Register forced failure
        String orderId = "dlq-test-order-" + UUID.randomUUID();
        paymentProcessingService.forceFail(orderId);

        // 2. Build and publish an OrderPlacedEvent
        OrderPlacedEvent orderEvent = new OrderPlacedEvent(
                orderId,
                "customer-dlq",
                "product-dlq",
                1,
                new BigDecimal("99.99"),
                LocalDateTime.now()
        );
        String payload = objectMapper.writeValueAsString(orderEvent);
        kafkaTemplate.send("orders.placed", orderId, payload);

        // 3. Wait for processing:
        //    PaymentProcessingException is non-retryable → goes straight to DLQ.
        //    We still give it enough time for the broker round-trip + error handler.
        //    (With retryable exceptions it would be 3 × 1s = 3s; non-retryable is faster.)
        Thread.sleep(10_000);

        // 4. Read from payments.dlq
        ConsumerRecords<String, String> dlqRecords = dlqConsumer.poll(Duration.ofSeconds(10));

        // 5. Assert — at least one message landed on the DLQ
        boolean foundOnDlq = false;
        for (ConsumerRecord<String, String> record : dlqRecords) {
            // The DLQ message value is the raw bytes of the original Kafka record payload
            // (serialized as String by StringDeserializer). It should contain the orderId.
            if (record.value() != null && record.value().contains(orderId)) {
                foundOnDlq = true;
                break;
            }
            // Also check the key — DeadLetterPublishingRecoverer preserves the original key
            if (orderId.equals(record.key())) {
                foundOnDlq = true;
                break;
            }
        }

        assertThat(foundOnDlq)
                .as("Expected original message to be routed to payments.dlq for orderId=%s", orderId)
                .isTrue();
    }
}
