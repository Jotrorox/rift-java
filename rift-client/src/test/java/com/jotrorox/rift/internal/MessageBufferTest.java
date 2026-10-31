package com.jotrorox.rift.internal;

import com.jotrorox.rift.Message;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageBufferTest {
    private static Message message(String payload) {
        return new Message("a", null, payload.getBytes());
    }

    @Test
    void deliversInOrderAndTimesOut() throws Exception {
        MessageBuffer buffer = new MessageBuffer(4);
        buffer.put(message("1"));
        buffer.put(message("2"));
        assertEquals("1", buffer.take(1, TimeUnit.SECONDS).payloadAsString());
        assertEquals("2", buffer.take(1, TimeUnit.SECONDS).payloadAsString());
        assertNull(buffer.take(10, TimeUnit.MILLISECONDS));
        assertFalse(buffer.isDrained());
    }

    @Test
    void blocksProducersWhenFull() throws Exception {
        MessageBuffer buffer = new MessageBuffer(1);
        buffer.put(message("1"));
        CompletableFuture<Boolean> second = CompletableFuture.supplyAsync(() -> {
            try {
                return buffer.put(message("2"));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(50);
        assertFalse(second.isDone());
        buffer.take(1, TimeUnit.SECONDS);
        assertTrue(second.get(1, TimeUnit.SECONDS));
    }

    @Test
    void drainsBeforeReportingTheEnd() throws Exception {
        MessageBuffer buffer = new MessageBuffer(4);
        buffer.put(message("1"));
        buffer.end(false);
        assertFalse(buffer.put(message("2")));
        assertEquals("1", buffer.take(1, TimeUnit.SECONDS).payloadAsString());
        assertNull(buffer.take(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
        assertTrue(buffer.isDrained());
    }

    @Test
    void discardingEndDropsMessagesAndWakesProducers() throws Exception {
        MessageBuffer buffer = new MessageBuffer(1);
        buffer.put(message("1"));
        CompletableFuture<Boolean> blocked = CompletableFuture.supplyAsync(() -> {
            try {
                return buffer.put(message("2"));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        Thread.sleep(50);
        buffer.end(true);
        assertFalse(blocked.get(1, TimeUnit.SECONDS));
        assertTrue(buffer.isDrained());
    }
}
