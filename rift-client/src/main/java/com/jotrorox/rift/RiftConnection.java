package com.jotrorox.rift;

import com.jotrorox.rift.control.RiftControl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A connection to Rift that reconnects on its own and restores subscriptions.
 *
 * <p>Long-running plugins should prefer this over a bare {@link RiftClient}:
 * Rift restarts, network interruptions and slow-consumer disconnects are
 * handled by reconnecting with exponential backoff and subscribing again.
 * Messages published while disconnected are not replayed; use retained
 * streams for work that must survive outages.
 *
 * <pre>{@code
 * RiftConnection rift = RiftConnection.builder(clientBuilder::connect).start();
 * rift.subscribe("game.chat", message -> broadcast(message.payloadAsString()));
 * rift.ready().thenRun(() -> log("connected to Rift"));
 * }</pre>
 */
public final class RiftConnection implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(RiftConnection.class.getName());

    private final Connector connector;
    private final Executor callbacks;
    private final Duration minDelay;
    private final Duration maxDelay;
    private final Listener listener;
    private final ScheduledExecutorService scheduler;
    private final Set<Registration> registrations = ConcurrentHashMap.newKeySet();
    private final CompletableFuture<RiftClient> ready = new CompletableFuture<>();
    private volatile RiftClient current;
    private volatile boolean closed;
    private int failures;

    private RiftConnection(Builder builder) {
        this.connector = builder.connector;
        this.callbacks = builder.callbacks;
        this.minDelay = builder.minDelay;
        this.maxDelay = builder.maxDelay;
        this.listener = builder.listener;
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "rift-connection");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.scheduler = executor;
    }

    /** Opens a client; typically {@code RiftClient.Builder::connect}. */
    @FunctionalInterface
    public interface Connector {
        RiftClient connect() throws IOException;
    }

    /** Observes connection changes. Callbacks run on the connection's own thread. */
    public interface Listener {
        default void connected(RiftClient client) {
        }

        default void disconnected(Throwable cause) {
        }

        default void connectFailed(IOException error, Duration retryIn) {
        }
    }

    public static Builder builder(Connector connector) {
        return new Builder(connector);
    }

    /** Starts connecting with default settings. */
    public static RiftConnection start(Connector connector) {
        return builder(connector).start();
    }

    /** Completes with the client once the first connection is established. */
    public CompletableFuture<RiftClient> ready() {
        return ready.copy();
    }

    /** The current client, if connected. */
    public Optional<RiftClient> client() {
        RiftClient client = current;
        return client != null && client.isConnected() ? Optional.of(client) : Optional.empty();
    }

    /**
     * The current client.
     *
     * @throws RiftException if Rift is not connected right now
     */
    public RiftClient requireClient() throws RiftException {
        return client().orElseThrow(() -> new RiftException("not connected to Rift"));
    }

    public boolean isConnected() {
        return client().isPresent();
    }

    /** Subscribes now and again after every reconnect. */
    public Registration subscribe(String pattern, MessageHandler handler) {
        return subscribe(pattern, null, handler);
    }

    /**
     * Subscribes now and again after every reconnect. The handler runs on the
     * callback executor configured in the builder.
     */
    public Registration subscribe(String pattern, String queueGroup, MessageHandler handler) {
        Subjects.requirePattern(pattern);
        Objects.requireNonNull(handler, "handler");
        Registration registration = new Registration(pattern, queueGroup, handler);
        registrations.add(registration);
        if (closed) {
            registration.close();
            return registration;
        }
        RiftClient client = current;
        if (client != null) {
            scheduler.execute(() -> registration.attach(client));
        }
        return registration;
    }

    /** Publishes through the current connection and waits for broker acceptance. */
    public void publish(String subject, byte[] payload) throws IOException {
        requireClient().publish(subject, payload);
    }

    /** Publishes UTF-8 text through the current connection. */
    public void publish(String subject, String payload) throws IOException {
        publish(subject, payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Sends a request through the current connection. */
    public Message request(String subject, byte[] payload, Duration timeout) throws IOException {
        return requireClient().request(subject, payload, timeout);
    }

    /**
     * Control API of the current connection.
     *
     * @throws RiftException if Rift is not connected right now
     */
    public RiftControl control() throws RiftException {
        return requireClient().control();
    }

    /** Stops reconnecting and closes the current client. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Registration registration : List.copyOf(registrations)) {
            registration.close();
        }
        scheduler.shutdownNow();
        RiftClient client = current;
        current = null;
        if (client != null) {
            client.close();
        }
        ready.completeExceptionally(new RiftException("connection closed before it was established"));
    }

    private void connect() {
        if (closed) {
            return;
        }
        RiftClient client;
        try {
            client = connector.connect();
        } catch (IOException | RuntimeException e) {
            IOException error = e instanceof IOException io ? io : new IOException(e.getMessage(), e);
            Duration delay = nextDelay();
            LOGGER.log(System.Logger.Level.WARNING, "could not connect to Rift, retrying in "
                    + delay.toMillis() + " ms: " + error.getMessage());
            notify(() -> listener.connectFailed(error, delay));
            schedule(this::connect, delay);
            return;
        }
        if (closed) {
            client.close();
            return;
        }
        failures = 0;
        current = client;
        for (Registration registration : registrations) {
            registration.attach(client);
        }
        notify(() -> listener.connected(client));
        ready.complete(client);
        client.closed().whenComplete((ignored, error) -> scheduler.execute(() -> disconnected(client)));
    }

    private void disconnected(RiftClient client) {
        if (closed || current != client) {
            return;
        }
        current = null;
        RiftException cause = new RiftException("connection to Rift lost");
        LOGGER.log(System.Logger.Level.WARNING, "connection to Rift lost, reconnecting");
        notify(() -> listener.disconnected(cause));
        schedule(this::connect, nextDelay());
    }

    private Duration nextDelay() {
        int exponent = Math.min(failures++, 16);
        long base = Math.min(maxDelay.toMillis(), minDelay.toMillis() << exponent);
        long jitter = ThreadLocalRandom.current().nextLong(base / 4 + 1);
        return Duration.ofMillis(Math.min(maxDelay.toMillis(), base + jitter));
    }

    private void schedule(Runnable task, Duration delay) {
        if (!closed) {
            try {
                scheduler.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RuntimeException ignored) {
                // Closed concurrently.
            }
        }
    }

    private void notify(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException e) {
            LOGGER.log(System.Logger.Level.WARNING, "Rift connection listener failed", e);
        }
    }

    /** A subscription that is restored after reconnects. Close it to unsubscribe. */
    public final class Registration implements AutoCloseable {
        private final String pattern;
        private final String queueGroup;
        private final MessageHandler handler;
        private volatile Subscription subscription;
        private volatile boolean active = true;

        private Registration(String pattern, String queueGroup, MessageHandler handler) {
            this.pattern = pattern;
            this.queueGroup = queueGroup;
            this.handler = handler;
        }

        public String pattern() {
            return pattern;
        }

        /** Whether messages are currently being received. */
        public boolean isSubscribed() {
            Subscription current = subscription;
            return active && current != null && current.isActive();
        }

        private void attach(RiftClient client) {
            if (!active || closed || client != current || isSubscribed()) {
                return;
            }
            try {
                Subscription created = client.subscribe(pattern, queueGroup, this::deliver);
                subscription = created;
                if (!active) {
                    created.close();
                    return;
                }
                created.closed().whenComplete((ignored, error) -> {
                    if (error != null && active && client == current && client.isConnected()) {
                        // Rift dropped only this subscription, e.g. as a slow consumer.
                        LOGGER.log(System.Logger.Level.WARNING, "subscription to " + pattern
                                + " ended, subscribing again: " + error.getMessage());
                        schedule(() -> attach(client), minDelay);
                    }
                });
            } catch (RiftPermissionException e) {
                LOGGER.log(System.Logger.Level.ERROR, "Rift denied the subscription to " + pattern
                        + "; check the principal's subscribe grants: " + e.getMessage());
            } catch (IOException | RuntimeException e) {
                LOGGER.log(System.Logger.Level.WARNING, "could not subscribe to " + pattern
                        + ", retrying: " + e.getMessage());
                Duration retry = minDelay.multipliedBy(4);
                schedule(() -> attach(client), retry.compareTo(maxDelay) > 0 ? maxDelay : retry);
            }
        }

        private void deliver(Message message) {
            if (!active) {
                return;
            }
            if (callbacks == null) {
                invoke(message);
                return;
            }
            try {
                callbacks.execute(() -> {
                    if (active) {
                        invoke(message);
                    }
                });
            } catch (RuntimeException e) {
                LOGGER.log(System.Logger.Level.WARNING, "could not dispatch message on " + message.subject(), e);
            }
        }

        private void invoke(Message message) {
            try {
                handler.onMessage(message);
            } catch (Exception e) {
                LOGGER.log(System.Logger.Level.WARNING, "message handler for " + pattern + " failed", e);
            }
        }

        @Override
        public void close() {
            active = false;
            registrations.remove(this);
            Subscription current = subscription;
            if (current != null) {
                current.close();
            }
        }
    }

    /** Builder for {@link RiftConnection}. */
    public static final class Builder {
        private final Connector connector;
        private Executor callbacks;
        private Duration minDelay = Duration.ofSeconds(1);
        private Duration maxDelay = Duration.ofSeconds(30);
        private Listener listener = new Listener() {
        };

        private Builder(Connector connector) {
            this.connector = Objects.requireNonNull(connector, "connector");
        }

        /**
         * Runs subscription handlers on {@code executor}, for example a game
         * server's main thread. By default handlers run on each subscription's
         * delivery thread.
         */
        public Builder callbackExecutor(Executor executor) {
            this.callbacks = Objects.requireNonNull(executor, "executor");
            return this;
        }

        /** Backoff bounds between reconnect attempts. Defaults to 1 to 30 seconds. */
        public Builder reconnectDelay(Duration min, Duration max) {
            if (min.isNegative() || min.isZero() || max.compareTo(min) < 0) {
                throw new IllegalArgumentException("reconnect delays must be positive and min <= max");
            }
            this.minDelay = min;
            this.maxDelay = max;
            return this;
        }

        public Builder listener(Listener listener) {
            this.listener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /** Starts connecting in the background and returns immediately. */
        public RiftConnection start() {
            RiftConnection connection = new RiftConnection(this);
            connection.scheduler.execute(connection::connect);
            return connection;
        }
    }
}
