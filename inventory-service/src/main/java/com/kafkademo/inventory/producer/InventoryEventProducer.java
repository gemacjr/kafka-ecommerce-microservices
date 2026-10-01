package com.kafkademo.inventory.producer;

import com.kafkademo.inventory.model.StockReservedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockReserved(StockReservedEvent event) {
        log.info("Publishing StockReservedEvent for orderId={}, productId={}, status={}",
                event.getOrderId(), event.getProductId(), event.getStatus());

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(INVENTORY_RESERVED_TOPIC, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish StockReservedEvent for orderId={}: {}",
                        event.getOrderId(), ex.getMessage(), ex);
            } else {
                log.info("StockReservedEvent published successfully for orderId={} | " +
                                "Topic={}, Partition={}, Offset={}",
                        event.getOrderId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
