package com.kafkademo.inventory.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.inventory.model.OrderPlacedEvent;
import com.kafkademo.inventory.model.StockReservedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
@EmbeddedKafka(
        partitions = 3,
        topics = {"orders.placed", "orders.cancelled", "inventory.reserved"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
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
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(Collections.singletonList("inventory.reserved"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void whenOrderPlacedForAvailableProduct_thenStockReservedEventPublished() throws Exception {
        // Arrange
        String orderId = "test-order-" + UUID.randomUUID();
        OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
                orderId,
                "customer-001",
                "prod-1",   // prod-1 has 100 units in stock — plenty
                2,
                new BigDecimal("29.99"),
                LocalDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(orderPlacedEvent);

        // Act — publish to orders.placed topic
        kafkaTemplate.send("orders.placed", orderId, payload);

        // Wait for the consumer to process the event and publish to inventory.reserved
        Thread.sleep(3000);

        // Assert — read from inventory.reserved and verify the event
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));

        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        boolean foundMatchingRecord = false;
        for (ConsumerRecord<String, String> record : records) {
            if (orderId.equals(record.key())) {
                StockReservedEvent reservedEvent = objectMapper.readValue(record.value(), StockReservedEvent.class);
                assertThat(reservedEvent.getOrderId()).isEqualTo(orderId);
                assertThat(reservedEvent.getProductId()).isEqualTo("prod-1");
                assertThat(reservedEvent.getStatus()).isEqualTo("RESERVED");
                assertThat(reservedEvent.getAvailableStock()).isGreaterThanOrEqualTo(0);
                assertThat(reservedEvent.getReservedAt()).isNotNull();
                foundMatchingRecord = true;
                break;
            }
        }

        assertThat(foundMatchingRecord)
                .as("Expected to find a StockReservedEvent with orderId=%s and status=RESERVED", orderId)
                .isTrue();
    }

    @Test
    void whenOrderPlacedForLowStockProduct_thenInsufficientEventPublished() throws Exception {
        // Arrange — prod-2 has only 5 units, we request 10
        String orderId = "test-order-low-" + UUID.randomUUID();
        OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
                orderId,
                "customer-002",
                "prod-2",
                10,   // more than available stock of 5
                new BigDecimal("99.99"),
                LocalDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(orderPlacedEvent);

        // Act
        kafkaTemplate.send("orders.placed", orderId, payload);
        Thread.sleep(3000);

        // Assert
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));

        boolean foundMatchingRecord = false;
        for (ConsumerRecord<String, String> record : records) {
            if (orderId.equals(record.key())) {
                StockReservedEvent reservedEvent = objectMapper.readValue(record.value(), StockReservedEvent.class);
                assertThat(reservedEvent.getOrderId()).isEqualTo(orderId);
                assertThat(reservedEvent.getStatus()).isEqualTo("INSUFFICIENT");
                foundMatchingRecord = true;
                break;
            }
        }

        assertThat(foundMatchingRecord)
                .as("Expected to find a StockReservedEvent with orderId=%s and status=INSUFFICIENT", orderId)
                .isTrue();
    }
}
