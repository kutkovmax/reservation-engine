package com.reservation.inventory.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.retry.backoff.BackOffContext;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SequenceBackOffPolicyTest {

    @Test
    void appliesConfiguredRetryIntervalsInOrder() {
        List<Long> recordedIntervals = new ArrayList<>();
        SequenceBackOffPolicy policy = new SequenceBackOffPolicy(1_000, 5_000, 30_000)
            .withSleeper(recordedIntervals::add);
        BackOffContext context = policy.start(null);

        policy.backOff(context);
        policy.backOff(context);
        policy.backOff(context);

        assertThat(recordedIntervals).containsExactly(1_000L, 5_000L, 30_000L);
    }

    @Test
    void configuresThreeRetryTopicsAndInactiveDltHandler() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
        RetryTopicConfiguration configuration = new KafkaRetryConfiguration()
            .orderEventsRetryTopicConfiguration(kafkaTemplate);

        assertThat(configuration.getDestinationTopicProperties())
            .filteredOn(DestinationTopic.Properties::isRetryTopic)
            .extracting(DestinationTopic.Properties::delay)
            .containsExactly(1_000L, 5_000L, 30_000L);
        assertThat(configuration.getDestinationTopicProperties())
            .filteredOn(DestinationTopic.Properties::isDltTopic)
            .singleElement()
            .satisfies(properties -> assertThat(properties.autoStartDltHandler()).isFalse());
    }
}
