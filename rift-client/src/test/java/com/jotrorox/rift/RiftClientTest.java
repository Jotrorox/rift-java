package com.jotrorox.rift;

import com.jotrorox.rift.stream.ConsumerConfig;
import com.jotrorox.rift.stream.Delivery;
import com.jotrorox.rift.stream.StreamConsumer;
import com.jotrorox.rift.testing.FakeRift;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Client behavior against the in-memory {@link FakeRift} server. */
class RiftClientTest {
    private static final Duration WAIT = Duration.ofSeconds(2);

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void authenticatesWithTheToken() throws IOException {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect()) {
            assertTrue(client.isConnected());
        }
        assertThrows(RiftPermissionException.class,
                () -> RiftClient.over(rift.transport(), "wrong", Duration.ofSeconds(1)));
    }

    @Test
    void publishesAndSubscribes() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect(); Subscription subscription = client.subscribe("game.*")) {
            client.publish("game.join", "Steve");
            client.publish("other.join", "ignored");
            client.publish("game.quit", "_INBOX.me", utf8("Alex"));
            assertEquals(new Message("game.join", null, utf8("Steve")), subscription.next());
            Message quit = subscription.poll(WAIT);
            assertEquals("Alex", quit.payloadAsString());
            assertEquals("_INBOX.me", quit.replySubject().orElseThrow());
            assertNull(subscription.poll(Duration.ofMillis(50)));
            assertEquals("game.*", subscription.pattern());
            assertNull(subscription.queueGroup());
        }
    }

    @Test
    void handlersReceiveInOrderAndSurviveExceptions() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect()) {
            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(3);
            Subscription subscription = client.subscribe("game.>", message -> {
                received.add(message.payloadAsString());
                done.countDown();
                if (message.payloadAsString().equals("1")) {
                    throw new IllegalStateException("handler failure");
                }
            });
            try (Publisher publisher = client.publisher()) {
                for (String payload : List.of("1", "2", "3")) {
                    publisher.publish("game.x", payload);
                }
                publisher.flush();
            }
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(List.of("1", "2", "3"), received);
            assertTrue(subscription.isActive());
            assertThrows(IllegalStateException.class, subscription::next);
        }
    }

    @Test
    void pipelinedErrorsSurfaceOnFlush() throws Exception {
        FakeRift rift = new FakeRift().deny("secret.");
        try (RiftClient client = rift.connect()) {
            Publisher publisher = client.publisher();
            publisher.publish("game.ok", "1");
            publisher.publish("secret.data", "2");
            assertThrows(RiftPermissionException.class, publisher::flush);
            assertFalse(publisher.isUsable());
            assertThrows(RiftException.class, () -> publisher.publish("game.ok", "3"));
            assertEquals(List.of("game.ok"), rift.published().stream().map(Message::subject).toList());
        }
    }

    @Test
    void sharedPublisherRecoversAfterFailure() throws Exception {
        FakeRift rift = new FakeRift().deny("secret.");
        try (RiftClient client = rift.connect()) {
            assertThrows(RiftPermissionException.class, () -> client.publish("secret.x", "1"));
            client.publish("game.ok", "2");
            assertEquals(1, rift.published().size());
        }
    }

    @Test
    void validatesArgumentsBeforeSending() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect()) {
            assertThrows(IllegalArgumentException.class, () -> client.publish("game.*", "x"));
            assertThrows(IllegalArgumentException.class, () -> client.subscribe("game..x"));
            assertThrows(IllegalArgumentException.class, () -> client.subscribe("game.x", "bad group", null));
            assertThrows(IllegalArgumentException.class,
                    () -> client.publish("game.x", new byte[1024 * 1024 + 1]));
            assertThrows(IllegalArgumentException.class,
                    () -> client.request("game.x", new byte[0], Duration.ZERO));
            assertThrows(IllegalArgumentException.class,
                    () -> client.request("game.x", new byte[0], Duration.ofSeconds(121)));
            assertThrows(IllegalArgumentException.class,
                    () -> client.consumer("jobs", "has space", ConsumerConfig.of(">")));
            assertTrue(rift.published().isEmpty());
        }
    }

    @Test
    void requestsGetReplies() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect()) {
            client.subscribe("math.double", message -> client.publish(message.reply(),
                    Integer.toString(2 * Integer.parseInt(message.payloadAsString()))));
            assertEquals("42", client.request("math.double", "21", WAIT).payloadAsString());
            CompletableFuture<Message> async = client.requestAsync("math.double", utf8("5"), WAIT);
            assertEquals("10", async.get(2, TimeUnit.SECONDS).payloadAsString());

            RiftRejectedException none = assertThrows(RiftRejectedException.class,
                    () -> client.request("math.none", new byte[0], WAIT));
            assertEquals("no responders", none.getMessage());
            ExecutionException failed = assertThrows(ExecutionException.class,
                    () -> client.requestAsync("math.none", new byte[0], WAIT).get(2, TimeUnit.SECONDS));
            assertInstanceOf(RiftRejectedException.class, failed.getCause());
        }
    }

    @Test
    void queueGroupsDeliverOnce() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect();
             Subscription first = client.subscribe("jobs", "workers", null);
             Subscription second = client.subscribe("jobs", "workers", null);
             Subscription observer = client.subscribe("jobs")) {
            client.publish("jobs", "1");
            assertNotNull(observer.poll(WAIT));
            Message a = first.poll(Duration.ofMillis(200));
            Message b = second.poll(Duration.ofMillis(200));
            assertTrue(a == null ^ b == null);
            assertEquals("workers", first.queueGroup());
        }
    }

    @Test
    void operationsTimeOut() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect(Duration.ofMillis(300))) {
            rift.silent(true);
            long started = System.nanoTime();
            RiftTimeoutException timeout = assertThrows(RiftTimeoutException.class,
                    () -> client.subscribe("game.x"));
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(2)) < 0);
            assertEquals("subscribe timed out", timeout.getMessage());

            Publisher publisher = client.publisher();
            assertThrows(RiftTimeoutException.class, publisher::flush);
            assertFalse(publisher.isUsable());
        }
    }

    @Test
    void subscriptionsEndWhenTheConnectionCloses() throws Exception {
        FakeRift rift = new FakeRift();
        RiftClient client = rift.connect();
        Subscription subscription = client.subscribe("game.x");
        client.publish("game.x", "buffered");
        assertNotNull(subscription.poll(WAIT));
        client.close();
        assertTrue(client.closed().isDone());
        ExecutionException ended = assertThrows(ExecutionException.class, () -> subscription.closed().get());
        assertInstanceOf(RiftException.class, ended.getCause());
        RiftException next = assertThrows(RiftException.class, subscription::next);
        assertNotNull(next.getCause());
        assertThrows(RiftException.class, () -> client.subscribe("game.y"));
        client.close();
    }

    @Test
    void closingASubscriptionStopsDelivery() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient client = rift.connect()) {
            Subscription subscription = client.subscribe("game.x");
            subscription.close();
            assertFalse(subscription.isActive());
            subscription.closed().get(1, TimeUnit.SECONDS);
            RiftException ended = assertThrows(RiftException.class, () -> subscription.poll(WAIT));
            assertNull(ended.getCause());
            assertTrue(client.isConnected());
        }
    }

    @Test
    void streamsStoreAndFetch() throws Exception {
        FakeRift rift = new FakeRift().stream("jobs");
        try (RiftClient client = rift.connect()) {
            assertEquals(1, client.streamPublish("jobs", "jobs.a", utf8("first")).sequence());
            assertEquals(2, client.streamPublish("jobs", "jobs.b", "_INBOX.r", utf8("second")).sequence());
            StreamConsumer consumer = client.consumer("jobs", "worker", ConsumerConfig.of("jobs.>"));
            assertEquals("jobs", consumer.stream());
            assertEquals("worker", consumer.name());
            List<Delivery> deliveries = consumer.fetch(1);
            assertEquals(1, deliveries.size());
            Delivery first = deliveries.get(0);
            assertEquals(1, first.sequence());
            assertEquals(1, first.deliveryCount());
            assertFalse(first.isRedelivery());
            assertEquals(1_700_000_000_000L, first.timestamp().toEpochMilli());
            assertEquals("first", first.message().payloadAsString());
            consumer.ack(first);
            Delivery second = consumer.fetch(10).get(0);
            assertEquals("_INBOX.r", second.message().reply());
            assertThrows(IllegalArgumentException.class, () -> consumer.fetch(0));
            assertThrows(IllegalArgumentException.class, () -> consumer.fetch(257));
            assertThrows(RiftRejectedException.class, () -> client.streamPublish("missing", "jobs.a", new byte[0]));
        }
    }
}
