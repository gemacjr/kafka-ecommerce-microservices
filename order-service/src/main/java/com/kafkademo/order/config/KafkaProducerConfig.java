package com.kafkademo.order.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Idempotent producer settings
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Prevent type header issues across services
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // --- Exactly-once semantics (EOS) ---
    //
    // EOS requires a transactional producer.  We create a *separate* ProducerFactory
    // and KafkaTemplate rather than enabling transactions on the existing one, because:
    //   1. A transactional factory wraps every send() in begin/commitTransaction,
    //      which would break the existing record-at-a-time producers and their tests.
    //   2. This keeps EOS opt-in: only TransactionalOrderEventProducer uses it.
    //
    // transactionIdPrefix uniquely identifies this producer instance to the broker.
    // With ENABLE_IDEMPOTENCE=true the broker deduplicates retried sends within
    // a transaction (the "exactly-once" guarantee).

    @Bean
    public ProducerFactory<String, Object> transactionalProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(configProps);
        // transactionIdPrefix activates Kafka transactions on this factory.
        // Each producer instance gets a unique ID: "order-tx-0", "order-tx-1", ...
        factory.setTransactionIdPrefix("order-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    // KafkaTransactionManager integrates Kafka transactions with Spring's
    // @Transactional mechanism.  Injected into TransactionalOrderEventProducer
    // so that executeInTransaction() can coordinate the transaction lifecycle.
    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }
}
