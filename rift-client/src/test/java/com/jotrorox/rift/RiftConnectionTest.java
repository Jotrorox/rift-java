package com.jotrorox.rift;

import com.jotrorox.rift.decision.Decision;
import com.jotrorox.rift.testing.FakeRift;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiftConnectionTest {
    private static final Duration FAST = Duration.ofMillis(20);

    private static RiftConnection.Builder builder(FakeRift rift) {
        return RiftConnection.builder(rift::connect).reconnectDelay(FAST, Duration.ofMillis(100));
    }

    private static void eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not reached in time");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void connectsAndDelivers() throws Exception {
        FakeRift rift = new FakeRift();
        LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();
        try (RiftConnection connection = builder(rift).start()) {
            RiftConnection.Registration registration =
                    connection.subscribe("game.chat", message -> received.add(message.payloadAsString()));
            connection.ready().get(5, TimeUnit.SECONDS);
            eventually(registration::isSubscribed);
            connection.publish("game.chat", "hello");
            assertEquals("hello", received.poll(5, TimeUnit.SECONDS));
            assertTrue(connection.isConnected());
        }
    }

    @Test
    void reconnectsAndRestoresSubscriptions() throws Exception {
        FakeRift rift = new FakeRift();
        LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();
        List<String> events = new CopyOnWriteArrayList<>();
        RiftConnection.Listener listener = new RiftConnection.Listener() {
            @Override
            public void connected(RiftClient client) {
                events.add("connected");
            }

            @Override
            public void disconnected(Throwable cause) {
                events.add("disconnected");
            }

            @Override
            public void connectFailed(IOException error, Duration retryIn) {
                events.add("failed");
            }
        };
        try (RiftConnection connection = builder(rift).listener(listener).start()) {
            RiftConnection.Registration registration =
                    connection.subscribe("game.chat", message -> received.add(message.payloadAsString()));
            connection.ready().get(5, TimeUnit.SECONDS);
            eventually(registration::isSubscribed);

            rift.offline(true);
            rift.disconnectAll();
            eventually(() -> !connection.isConnected());
            assertThrows(RiftException.class, () -> connection.publish("game.chat", "lost"));
            eventually(() -> events.contains("failed"));

            rift.offline(false);
            eventually(() -> connection.isConnected() && registration.isSubscribed());
            connection.publish("game.chat", "back");
            assertEquals("back", received.poll(5, TimeUnit.SECONDS));
            assertEquals(1, rift.subscriptions("game.chat"));
            assertEquals("connected", events.get(0));
            assertTrue(events.indexOf("disconnected") > 0);
            assertEquals("connected", events.get(events.size() - 1));
        }
    }

    @Test
    void retriesUntilRiftIsReachable() throws Exception {
        FakeRift rift = new FakeRift();
        rift.offline(true);
        AtomicInteger attempts = new AtomicInteger();
        try (RiftConnection connection = RiftConnection.builder(() -> {
                    attempts.incrementAndGet();
                    return rift.connect();
                })
                .reconnectDelay(FAST, Duration.ofMillis(50))
                .start()) {
            eventually(() -> attempts.get() >= 3);
            assertFalse(connection.ready().isDone());
            rift.offline(false);
            connection.ready().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void runsHandlersOnTheCallbackExecutor() throws Exception {
        FakeRift rift = new FakeRift();
        ExecutorService main = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "main-thread"));
        LinkedBlockingQueue<String> threads = new LinkedBlockingQueue<>();
        try (RiftConnection connection = builder(rift).callbackExecutor(main).start()) {
            RiftConnection.Registration registration =
                    connection.subscribe("game.x", message -> threads.add(Thread.currentThread().getName()));
            connection.ready().get(5, TimeUnit.SECONDS);
            eventually(registration::isSubscribed);
            connection.publish("game.x", "1");
            assertEquals("main-thread", threads.poll(5, TimeUnit.SECONDS));
        } finally {
            main.shutdownNow();
        }
    }

    @Test
    void decidersAnswerAgainAfterReconnecting() throws Exception {
        FakeRift rift = new FakeRift();
        ExecutorService main = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "main-thread"));
        LinkedBlockingQueue<String> threads = new LinkedBlockingQueue<>();
        String login = """
                {"hook":"login","uuid":"069a79f444e94726a5befca90e38aaf5","name":"Notch",
                 "connection_id":"7","peer_ip":"203.0.113.4","hostname":"play.example","protocol":772}""";
        try (RiftConnection connection = builder(rift).callbackExecutor(main).start();
             RiftClient proxy = rift.connect()) {
            RiftConnection.Registration registration = connection.decide("relaxoban.login", request -> {
                threads.add(Thread.currentThread().getName());
                return CompletableFuture.completedFuture(Decision.deny("Banned " + request.name()));
            });
            assertEquals(RiftClient.DEFAULT_DECIDER_GROUP, registration.queueGroup());
            connection.ready().get(5, TimeUnit.SECONDS);
            eventually(registration::isSubscribed);
            assertEquals("{\"action\":\"deny\",\"reason\":\"Banned Notch\"}",
                    proxy.request("relaxoban.login", login, Duration.ofSeconds(2)).payloadAsString());
            // Decisions skip the callback executor, so a busy game thread does not delay logins.
            assertFalse(threads.take().equals("main-thread"));

            RiftClient before = connection.requireClient();
            rift.disconnectAll();
            eventually(() -> connection.client().filter(client -> client != before).isPresent()
                    && registration.isSubscribed());
            try (RiftClient after = rift.connect()) {
                assertEquals("{\"action\":\"deny\",\"reason\":\"Banned Notch\"}",
                        after.request("relaxoban.login", login, Duration.ofSeconds(2)).payloadAsString());
            }
            assertEquals(1, rift.subscriptions("relaxoban.login"));

            registration.close();
            eventually(() -> rift.subscriptions("relaxoban.login") == 0);
        } finally {
            main.shutdownNow();
        }
    }

    @Test
    void decidersNeedLiteralSubjects() {
        try (RiftConnection connection = builder(new FakeRift()).start()) {
            assertThrows(IllegalArgumentException.class,
                    () -> connection.decide("relaxoban.*", request -> null));
        }
    }

    @Test
    void closingARegistrationUnsubscribes() throws Exception {
        FakeRift rift = new FakeRift();
        LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();
        try (RiftConnection connection = builder(rift).start()) {
            RiftConnection.Registration registration =
                    connection.subscribe("game.x", message -> received.add(message.payloadAsString()));
            connection.ready().get(5, TimeUnit.SECONDS);
            eventually(registration::isSubscribed);
            registration.close();
            assertFalse(registration.isSubscribed());
            eventually(() -> rift.subscriptions("game.x") == 0);
            connection.publish("game.x", "1");
            assertNull(received.poll(100, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void closingStopsEverything() throws Exception {
        FakeRift rift = new FakeRift();
        RiftConnection connection = builder(rift).start();
        RiftClient client = connection.ready().get(5, TimeUnit.SECONDS);
        connection.close();
        assertFalse(client.isConnected());
        assertFalse(connection.isConnected());
        assertThrows(RiftException.class, connection::control);

        RiftConnection never = RiftConnection.builder(() -> {
            throw new IOException("unreachable");
        }).start();
        never.close();
        assertTrue(never.ready().isCompletedExceptionally());
    }
}
