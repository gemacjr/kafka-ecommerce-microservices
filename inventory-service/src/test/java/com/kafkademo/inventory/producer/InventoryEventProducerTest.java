package com.kafkademo.inventory.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
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
@EmbeddedKafka(
        partitions = 3,
        topics = {"inventory.reserved"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
class InventoryEventProducerTest {

    @Autowired
    private InventoryEventProducer inventoryEventProducer;

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
    void whenPublishStockReserved_thenEventAppearsInTopic() throws Exception {
        // Arrange
        String orderId = "producer-test-order-" + UUID.randomUUID();
        StockReservedEvent event = new StockReservedEvent(
                orderId,
                "prod-1",
                "RESERVED",
                98,
                LocalDateTime.now()
        );

        // Act
        inventoryEventProducer.publishStockReserved(event);

        // Wait for the message to be produced and available
        Thread.sleep(2000);

        // Assert — read from the topic and verify key and value
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));

        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        boolean foundRecord = false;
        for (ConsumerRecord<String, String> record : records) {
            if (orderId.equals(record.key())) {
                // Verify the key matches the orderId
                assertThat(record.key()).isEqualTo(orderId);

                // Parse the value and verify fields
                StockReservedEvent receivedEvent = objectMapper.readValue(record.value(), StockReservedEvent.class);
                assertThat(receivedEvent.getOrderId()).isEqualTo(orderId);
                assertThat(receivedEvent.getProductId()).isEqualTo("prod-1");
                assertThat(receivedEvent.getStatus()).isEqualTo("RESERVED");
                assertThat(receivedEvent.getAvailableStock()).isEqualTo(98);
                assertThat(receivedEvent.getReservedAt()).isNotNull();

                foundRecord = true;
                break;
            }
        }

        assertThat(foundRecord)
                .as("Expected to find StockReservedEvent with key=%s in inventory.reserved topic", orderId)
                .isTrue();
    }

    @Test
    void whenPublishInsufficientStockEvent_thenEventAppearsInTopic() throws Exception {
        // Arrange
        String orderId = "producer-test-insufficient-" + UUID.randomUUID();
        StockReservedEvent event = new StockReservedEvent(
                orderId,
                "prod-2",
                "INSUFFICIENT",
                5,
                LocalDateTime.now()
        );

        // Act
        inventoryEventProducer.publishStockReserved(event);
        Thread.sleep(2000);

        // Assert
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));

        boolean foundRecord = false;
        for (ConsumerRecord<String, String> record : records) {
            if (orderId.equals(record.key())) {
                assertThat(record.key()).isEqualTo(orderId);

                // Verify the raw JSON contains expected values (as a string check)
                String rawValue = record.value();
                assertThat(rawValue).contains("INSUFFICIENT");
                assertThat(rawValue).contains(orderId);
                assertThat(rawValue).contains("prod-2");

                foundRecord = true;
                break;
            }
        }

        assertThat(foundRecord)
                .as("Expected to find INSUFFICIENT StockReservedEvent with key=%s", orderId)
                .isTrue();
    }
}
