package com.kafkademo.payment.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.payment.model.PaymentProcessedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {"payments.processed"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@DirtiesContext
class PaymentEventProducerTest {

    @Autowired
    private PaymentEventProducer paymentEventProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "producer-test-group-" + UUID.randomUUID(),
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
    void whenPublishPaymentProcessed_thenMessageAppearsOnTopic() throws Exception {
        // Arrange
        String orderId = "producer-test-order-" + UUID.randomUUID();
        String paymentId = UUID.randomUUID().toString();

        PaymentProcessedEvent event = new PaymentProcessedEvent(
                orderId,
                paymentId,
                "SUCCESS",
                null,
                LocalDateTime.now()
        );

        // Act
        paymentEventProducer.publishPaymentProcessed(event);

        // Wait briefly for the async send to complete
        Thread.sleep(1000);

        // Assert — read from payments.processed
        ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofSeconds(5));

        boolean found = false;
        for (ConsumerRecord<String, String> record : records) {
            // Verify the message key is the orderId
            if (orderId.equals(record.key())) {
                found = true;
                assertThat(record.key()).isEqualTo(orderId);
                assertThat(record.value()).contains("SUCCESS");

                PaymentProcessedEvent received =
                        objectMapper.readValue(record.value(), PaymentProcessedEvent.class);
                assertThat(received.getOrderId()).isEqualTo(orderId);
                assertThat(received.getPaymentId()).isEqualTo(paymentId);
                assertThat(received.getStatus()).isEqualTo("SUCCESS");
                break;
            }
        }

        assertThat(found)
                .as("Expected a PaymentProcessedEvent on payments.processed with key=%s", orderId)
                .isTrue();
    }
}
