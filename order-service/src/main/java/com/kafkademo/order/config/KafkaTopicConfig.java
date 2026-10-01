package com.kafkademo.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersPlacedTopic() {
        return TopicBuilder.name("orders.placed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersCancelledTopic() {
        return TopicBuilder.name("orders.cancelled")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
