package com.jotrorox.rift.stream;

import com.jotrorox.rift.Subjects;
import com.jotrorox.rift.internal.Protocol;

import java.time.Duration;
import java.util.Objects;

/**
 * Settings of a durable stream consumer. Reopen a consumer with the same
 * settings to resume where it left off.
 *
 * @param filterSubject  subject pattern selecting records; must be covered by the principal's subscribe grants
 * @param startSequence  first sequence to deliver when the consumer is created
 * @param ackWait        how long a fetched record stays reserved before it is delivered again
 * @param maxAckPending  maximum number of fetched but unacknowledged records
 */
public record ConsumerConfig(String filterSubject, long startSequence, Duration ackWait, int maxAckPending) {
    public static final long DEFAULT_START_SEQUENCE = 1;
    public static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(30);
    public static final int DEFAULT_MAX_ACK_PENDING = 128;

    public ConsumerConfig {
        Subjects.requirePattern(filterSubject);
        Objects.requireNonNull(ackWait, "ackWait");
        if (startSequence < 1) {
            throw new IllegalArgumentException("startSequence must be at least 1");
        }
        long millis = ackWait.toMillis();
        if (millis < 1 || millis > Protocol.MAX_ACK_WAIT_MILLIS || ackWait.toNanos() % 1_000_000 != 0) {
            throw new IllegalArgumentException("ackWait must be 1 ms to 1 hour in whole milliseconds");
        }
        if (maxAckPending < 1 || maxAckPending > Protocol.MAX_ACK_PENDING) {
            throw new IllegalArgumentException("maxAckPending must be 1..65536");
        }
    }

    /** A consumer of {@code filterSubject} with default limits, starting at the first record. */
    public static ConsumerConfig of(String filterSubject) {
        return builder(filterSubject).build();
    }

    public static Builder builder(String filterSubject) {
        return new Builder(filterSubject);
    }

    /** Builder for {@link ConsumerConfig}. */
    public static final class Builder {
        private final String filterSubject;
        private long startSequence = DEFAULT_START_SEQUENCE;
        private Duration ackWait = DEFAULT_ACK_WAIT;
        private int maxAckPending = DEFAULT_MAX_ACK_PENDING;

        private Builder(String filterSubject) {
            this.filterSubject = filterSubject;
        }

        public Builder startSequence(long startSequence) {
            this.startSequence = startSequence;
            return this;
        }

        public Builder ackWait(Duration ackWait) {
            this.ackWait = ackWait;
            return this;
        }

        public Builder maxAckPending(int maxAckPending) {
            this.maxAckPending = maxAckPending;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(filterSubject, startSequence, ackWait, maxAckPending);
        }
    }
}
