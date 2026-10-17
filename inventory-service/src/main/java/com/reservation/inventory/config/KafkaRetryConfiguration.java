package com.reservation.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.List;

@Configuration(proxyBeanMethods = false)
@EnableKafka
public class KafkaRetryConfiguration extends RetryTopicConfigurationSupport {

    private static final String ORDER_EVENTS_TOPIC = "order-events";

    @Bean
    TaskScheduler retryTopicTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-retry-");
        return scheduler;
    }

    @Bean
    RetryTopicConfiguration orderEventsRetryTopicConfiguration(
        KafkaTemplate<String, Object> kafkaTemplate
    ) {
        return RetryTopicConfigurationBuilder.newInstance()
            .includeTopic(ORDER_EVENTS_TOPIC)
            .maxAttempts(4)
            .customBackoff(new SequenceBackOffPolicy(1_000, 5_000, 30_000))
            .retryTopicSuffix("-retry")
            .dltSuffix("-dlt")
            .suffixTopicsWithIndexValues()
            .doNotAutoCreateRetryTopics()
            .autoStartDltHandler(false)
            .create(kafkaTemplate);
    }

    @Override
    protected void manageNonBlockingFatalExceptions(
        List<Class<? extends Throwable>> nonBlockingFatalExceptions
    ) {
        nonBlockingFatalExceptions.clear();
    }
}
