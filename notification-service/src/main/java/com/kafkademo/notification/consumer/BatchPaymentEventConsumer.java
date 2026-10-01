package com.kafkademo.notification.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Demonstrates batch consumption: receives a full poll batch of payment events
 * (up to MAX_POLL_RECORDS=50) in a single method invocation instead of one
 * record at a time.  Spring Kafka delivers the batch as List<ConsumerRecord<K,V>>.
 *
 * <p>Contrast with PaymentEventConsumer (record-at-a-time, MANUAL_IMMEDIATE ack)
 * and InventoryEventConsumer — both use the default kafkaListenerContainerFactory.
 * This listener uses batchKafkaListenerContainerFactory with AckMode.BATCH so a
 * single offset commit covers the whole batch after all records are processed.
 *
 * <p>Typical use-cases for batch consumption:
 * <ul>
 *   <li>High-throughput analytics pipelines — process 50 events, bulk-insert into DB</li>
 *   <li>Aggregation — accumulate events before emitting a summary</li>
 *   <li>Rate-limited downstream calls — group records, send one HTTP batch request</li>
 * </ul>
 */
@Component
public class BatchPaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(BatchPaymentEventConsumer.class);

    private final ObjectMapper objectMapper;

    public BatchPaymentEventConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // containerFactory = "batchKafkaListenerContainerFactory" activates batch mode.
    // groupId overrides the factory's group so this consumer has its own offset pointer.
    @KafkaListener(
            topics = "payments.processed",
            groupId = "notification-batch-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatch(List<ConsumerRecord<String, String>> records) {
        log.info("[BATCH] Received {} payment event(s) in this poll cycle", records.size());

        int successCount = 0;
        int failureCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            try {
                JsonNode node = objectMapper.readTree(record.value());
                String status = node.path("status").asText();
                String orderId = node.path("orderId").asText();

                if ("SUCCESS".equals(status)) {
                    successCount++;
                } else {
                    failureCount++;
                }

                log.debug("[BATCH] partition={} offset={} orderId={} status={}",
                        record.partition(), record.offset(), orderId, status);
            } catch (Exception e) {
                log.error("[BATCH] Failed to parse record at offset={}: {}",
                        record.offset(), e.getMessage());
                // In batch mode, a single bad record does not prevent the rest of
                // the batch from being processed.  The batch offset commit still
                // happens after this method returns.  For production use you would
                // route bad records to a dead-letter topic here.
            }
        }

        // After processing all records in the batch, Spring commits offsets for
        // the entire batch in one round-trip (AckMode.BATCH).
        log.info("[BATCH] Batch summary — SUCCESS={}, FAILED={}, total={}",
                successCount, failureCount, records.size());
    }
}
