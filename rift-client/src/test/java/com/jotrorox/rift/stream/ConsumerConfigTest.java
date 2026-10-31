package com.jotrorox.rift.stream;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerConfigTest {
    @Test
    void usesRiftDefaults() {
        ConsumerConfig config = ConsumerConfig.of("game.jobs.>");
        assertEquals("game.jobs.>", config.filterSubject());
        assertEquals(1, config.startSequence());
        assertEquals(Duration.ofSeconds(30), config.ackWait());
        assertEquals(128, config.maxAckPending());
    }

    @Test
    void validatesLimits() {
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.of("game..jobs"));
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.builder("a").startSequence(0).build());
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.builder("a").ackWait(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
                () -> ConsumerConfig.builder("a").ackWait(Duration.ofHours(1).plusMillis(1)).build());
        assertThrows(IllegalArgumentException.class,
                () -> ConsumerConfig.builder("a").ackWait(Duration.ofNanos(1_500_000)).build());
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.builder("a").maxAckPending(0).build());
        assertThrows(IllegalArgumentException.class, () -> ConsumerConfig.builder("a").maxAckPending(65537).build());
        ConsumerConfig.builder("a").ackWait(Duration.ofHours(1)).maxAckPending(65536).build();
    }
}
