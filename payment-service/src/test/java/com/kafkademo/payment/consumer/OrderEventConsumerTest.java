package com.kafkademo.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.payment.model.OrderPlacedEvent;
import com.kafkademo.payment.model.PaymentProcessedEvent;
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

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"orders.placed", "payments.processed", "payments.dlq"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext
class OrderEventConsumerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-consumer-group-" + UUID.randomUUID(),
                "true",
                embeddedKafkaBroker
        );
        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(Collections.singletonList("payments.processed"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void whenOrderPlacedEventPublished_thenPaymentProcessedEventIsReceived() throws Exception {
        // Arrange
        String orderId = "test-order-" + UUID.randomUUID();
        OrderPlacedEvent orderEvent = new OrderPlacedEvent(
                orderId,
                "customer-001",
                "product-001",
                2,
                new BigDecimal("49.99"),
                LocalDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(orderEvent);

        // Act — publish the order event to trigger the consumer
        kafkaTemplate.send("orders.placed", orderId, payload);

        // Wait for the consumer to process and the producer to publish the result
        Thread.sleep(3000);

        // Assert — read from payments.processed
        ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofSeconds(5));

        boolean found = false;
        for (ConsumerRecord<String, String> record : records) {
            PaymentProcessedEvent processedEvent =
                    objectMapper.readValue(record.value(), PaymentProcessedEvent.class);
            if (orderId.equals(processedEvent.getOrderId())) {
                found = true;
                assertThat(processedEvent.getOrderId()).isEqualTo(orderId);
                assertThat(processedEvent.getStatus()).isIn("SUCCESS", "FAILED");
                assertThat(processedEvent.getPaymentId()).isNotBlank();
                assertThat(processedEvent.getProcessedAt()).isNotNull();
                break;
            }
        }

        assertThat(found)
                .as("Expected a PaymentProcessedEvent on payments.processed for orderId=%s", orderId)
                .isTrue();
    }
}
