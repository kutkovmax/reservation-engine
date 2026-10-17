package com.reservation.inventory.config;

import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.Sleeper;

import java.util.Arrays;

final class SequenceBackOffPolicy implements SleepingBackOffPolicy<SequenceBackOffPolicy> {

    private final long[] intervals;
    private final Sleeper sleeper;

    SequenceBackOffPolicy(long... intervals) {
        this(intervals, Thread::sleep);
    }

    private SequenceBackOffPolicy(long[] intervals, Sleeper sleeper) {
        if (intervals.length == 0 || Arrays.stream(intervals).anyMatch(interval -> interval < 0)) {
            throw new IllegalArgumentException("At least one non-negative backoff interval is required");
        }
        this.intervals = intervals.clone();
        this.sleeper = sleeper;
    }

    @Override
    public SequenceBackOffPolicy withSleeper(Sleeper sleeper) {
        return new SequenceBackOffPolicy(intervals, sleeper);
    }

    @Override
    public BackOffContext start(RetryContext context) {
        return new SequenceBackOffContext();
    }

    @Override
    public void backOff(BackOffContext context) throws BackOffInterruptedException {
        SequenceBackOffContext sequenceContext = (SequenceBackOffContext) context;
        int index = Math.min(sequenceContext.nextInterval, intervals.length - 1);
        sequenceContext.nextInterval++;

        try {
            sleeper.sleep(intervals[index]);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new BackOffInterruptedException("Interrupted while applying retry backoff", exception);
        }
    }

    private static final class SequenceBackOffContext implements BackOffContext {
        private int nextInterval;
    }
}
