package com.kafkademo.order.producer;

import com.kafkademo.order.model.OrderCancelledEvent;
import com.kafkademo.order.model.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Demonstrates exactly-once semantics (EOS) using Kafka transactions.
 *
 * <p>How EOS works in Kafka:
 * <ol>
 *   <li>{@code beginTransaction()} — broker reserves a transaction for this producer ID</li>
 *   <li>{@code send()} — records are written but not yet visible to consumers with
 *       {@code isolation.level=read_committed}</li>
 *   <li>{@code commitTransaction()} — broker atomically makes all records in the
 *       transaction visible; consumers see them all or none</li>
 *   <li>If the producer crashes before commit, the broker aborts the transaction;
 *       records are never exposed to downstream consumers</li>
 * </ol>
 *
 * <p>{@code executeInTransaction()} is a Spring helper that handles begin/commit/abort
 * automatically.  It is the preferred API over calling beginTransaction() manually.
 *
 * <p>Why a separate producer?  The transactional ProducerFactory (see KafkaProducerConfig)
 * sets {@code transactionIdPrefix}, which requires every send to be inside a transaction.
 * Using the same factory for non-transactional sends would throw an exception.
 *
 * <p>Use-case: atomically publish two events (placed + metadata) so downstream
 * services either receive both or neither — no partial state visible to consumers.
 */
@Component
public class TransactionalOrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionalOrderEventProducer.class);

    private static final String ORDERS_PLACED_TOPIC = "orders.placed";
    private static final String ORDERS_CANCELLED_TOPIC = "orders.cancelled";

    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

    public TransactionalOrderEventProducer(
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalKafkaTemplate) {
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    /**
     * Publishes an OrderPlacedEvent inside a Kafka transaction.
     *
     * <p>executeInTransaction() opens a transaction, runs the lambda, then:
     * <ul>
     *   <li>commits if the lambda returns normally</li>
     *   <li>aborts if the lambda throws</li>
     * </ul>
     * Downstream consumers configured with {@code isolation.level=read_committed}
     * (the default for Spring Kafka listeners) will not see the record until commit.
     */
    public void publishOrderPlacedTransactionally(OrderPlacedEvent event) {
        log.info("[EOS] Publishing OrderPlacedEvent transactionally for orderId: {}", event.getOrderId());

        transactionalKafkaTemplate.executeInTransaction(ops -> {
            // Both sends are part of the same atomic transaction.
            // If the second send fails, the broker rolls back both.
            var result = ops.send(ORDERS_PLACED_TOPIC, event.getOrderId(), event);
            log.info("[EOS] OrderPlacedEvent sent within transaction for orderId: {}", event.getOrderId());
            return result;
        });

        log.info("[EOS] Transaction committed for orderId: {}", event.getOrderId());
    }

    /**
     * Publishes an OrderCancelledEvent inside a Kafka transaction.
     */
    public void publishOrderCancelledTransactionally(OrderCancelledEvent event) {
        log.info("[EOS] Publishing OrderCancelledEvent transactionally for orderId: {}", event.getOrderId());

        transactionalKafkaTemplate.executeInTransaction(ops -> {
            var result = ops.send(ORDERS_CANCELLED_TOPIC, event.getOrderId(), event);
            log.info("[EOS] OrderCancelledEvent sent within transaction for orderId: {}", event.getOrderId());
            return result;
        });

        log.info("[EOS] Transaction committed for orderId: {}", event.getOrderId());
    }
}
