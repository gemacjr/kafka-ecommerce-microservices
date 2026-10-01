package com.kafkademo.order.producer;

import com.kafkademo.order.model.OrderPlacedEvent;
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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"orders.placed", "orders.cancelled"},
        brokerProperties = {"listeners=PLAINTEXT://localhost:19092", "port=19092"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
@ActiveProfiles("test")
class OrderEventProducerTest {

    @Autowired
    private OrderEventProducer orderEventProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "test-group-producer", "true", embeddedKafkaBroker);
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("orders.placed"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void publishOrderPlaced_shouldSendMessageToOrdersPlacedTopic() throws Exception {
        // Arrange
        String orderId = "test-order-" + System.currentTimeMillis();
        OrderPlacedEvent event = new OrderPlacedEvent(
                orderId,
                "customer-001",
                "product-001",
                2,
                new BigDecimal("49.99"),
                LocalDateTime.now()
        );

        // Act
        orderEventProducer.publishOrderPlaced(event);

        // Allow time for the async send to complete
        Thread.sleep(2000);

        // Assert — poll for records from the topic
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);

        assertThat(records).isNotEmpty();

        boolean found = false;
        for (ConsumerRecord<String, String> record : records) {
            if (orderId.equals(record.key())) {
                found = true;
                assertThat(record.value()).contains(orderId);
                break;
            }
        }

        assertThat(found)
                .as("Expected a record with key '%s' in orders.placed topic", orderId)
                .isTrue();
    }
}
