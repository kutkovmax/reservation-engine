package com.reservation.inventory.config;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfiguration {

    @Bean
    ProducerFactory<String, Object> kafkaProducerFactory(KafkaProperties kafkaProperties) {
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new JsonSerializer<>());

        return new DefaultKafkaProducerFactory<>(
            kafkaProperties.buildProducerProperties(null),
            new StringSerializer(),
            new DelegatingByTypeSerializer(delegates, true)
        );
    }

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic inventoryEventsTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("inventory-events")
            .partitions(3)
            .replicas(1)
            .build();
    }
}
