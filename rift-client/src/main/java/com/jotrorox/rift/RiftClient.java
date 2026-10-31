package com.jotrorox.rift;

import com.jotrorox.rift.control.RiftControl;
import com.jotrorox.rift.decision.Decision;
import com.jotrorox.rift.decision.DecisionHandler;
import com.jotrorox.rift.decision.DecisionRequest;
import com.jotrorox.rift.internal.Deadlines;
import com.jotrorox.rift.internal.Frame;
import com.jotrorox.rift.internal.FrameBuilder;
import com.jotrorox.rift.internal.Frames;
import com.jotrorox.rift.internal.Protocol;
import com.jotrorox.rift.internal.QuicTransport;
import com.jotrorox.rift.internal.TrustManagers;
import com.jotrorox.rift.internal.Transport;
import com.jotrorox.rift.stream.ConsumerConfig;
import com.jotrorox.rift.stream.Delivery;
import com.jotrorox.rift.stream.StoredRecord;
import com.jotrorox.rift.stream.StreamConsumer;

import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A connection to Rift's messaging bus.
 *
 * <p>One client holds one authenticated QUIC connection; every subscription,
 * request and publisher uses its own stream on it. Keep a single client open
 * for the lifetime of a plugin instead of reconnecting per message. Clients
 * are thread-safe.
 *
 * <pre>{@code
 * try (RiftClient client = RiftClient.builder()
 *         .address("127.0.0.1", 4222)
 *         .serverName("localhost")
 *         .trustCertificates(Path.of("messaging-cert.pem"))
 *         .tokenFromEnvironment("RIFT_PAPER_TOKEN")
 *         .connect()) {
 *     client.subscribe("game.player.*", message -> System.out.println(message.payloadAsString()));
 *     client.publish("game.player.join", "Steve");
 * }
 * }</pre>
 *
 * <p>All blocking methods must not be called from a game server's main thread.
 */
public final class RiftClient implements AutoCloseable {
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_SUBSCRIPTION_BUFFER = 1024;
    /** The queue group {@link #decide(String, DecisionHandler)} joins. */
    public static final String DEFAULT_DECIDER_GROUP = "deciders";

    private static final System.Logger LOGGER = System.getLogger(RiftClient.class.getName());

    private static final Duration REQUEST_TRANSPORT_GRACE = Duration.ofSeconds(5);

    private final Transport transport;
    private final Duration operationTimeout;
    private final int subscriptionBuffer;
    private final Deadlines deadlines = new Deadlines();
    private final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final Set<Publisher> publishers = ConcurrentHashMap.newKeySet();
    private final ReentrantLock sharedPublisherLock = new ReentrantLock();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final ExecutorService asyncExecutor;
    private final RiftControl control;
    private Publisher sharedPublisher;
    private volatile boolean closing;

    private RiftClient(Transport transport, Duration operationTimeout, int subscriptionBuffer) {
        this.transport = transport;
        this.operationTimeout = operationTimeout;
        this.subscriptionBuffer = subscriptionBuffer;
        this.asyncExecutor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "rift-async");
            thread.setDaemon(true);
            return thread;
        });
        this.control = new RiftControl(this);
        transport.onClose(this::connectionClosed);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Wraps an already authenticated transport. Intended for tests and custom
     * transports; applications should use {@link #builder()}.
     */
    public static RiftClient over(Transport transport, String token, Duration operationTimeout) throws IOException {
        return authenticate(transport, token, operationTimeout, DEFAULT_SUBSCRIPTION_BUFFER);
    }

    private static RiftClient authenticate(Transport transport, String token, Duration operationTimeout,
                                           int subscriptionBuffer) throws IOException {
        try (Deadlines deadlines = new Deadlines()) {
            Transport.Channel channel = transport.open();
            try (Deadlines.Deadline deadline = deadlines.arm(channel, operationTimeout)) {
                try {
                    Frames.write(channel.output(), new FrameBuilder(Protocol.AUTH).string(token).toByteArray());
                    channel.finish();
                    Frames.read(channel.input()).expectOk();
                } catch (IOException e) {
                    throw deadline.translate(e, "authentication");
                }
            }
        } catch (RiftException | RuntimeException e) {
            transport.close();
            throw e;
        } catch (IOException e) {
            transport.close();
            // Rift may close the connection without a reply when authentication fails.
            throw new RiftException("could not authenticate with Rift: " + e.getMessage(), e);
        }
        return new RiftClient(transport, operationTimeout, subscriptionBuffer);
    }

    /**
     * Publishes a message and waits until the broker accepted it. Uses a
     * shared publisher stream; for high message rates use {@link #publisher()}.
     */
    public void publish(String subject, byte[] payload) throws IOException {
        publish(subject, null, payload);
    }

    /** Publishes UTF-8 text and waits until the broker accepted it. */
    public void publish(String subject, String payload) throws IOException {
        publish(subject, null, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Publishes with a reply subject and waits until the broker accepted it.
     * Rift requires reply subjects to be literal {@code _INBOX.} subjects
     * covered by this principal's subscribe grants.
     */
    public void publish(String subject, String reply, byte[] payload) throws IOException {
        sharedPublisherLock.lock();
        try {
            if (sharedPublisher == null || !sharedPublisher.isUsable()) {
                // A failed shared publisher is replaced, never retried automatically.
                sharedPublisher = openPublisher(false);
            }
            sharedPublisher.publishConfirmed(subject, reply, payload);
        } finally {
            sharedPublisherLock.unlock();
        }
    }

    /** Opens a dedicated pipelined publisher. Close it when it is no longer needed. */
    public Publisher publisher() throws IOException {
        return openPublisher(true);
    }

    /** Subscribes to {@code pattern}; read messages with {@link Subscription#next()}. */
    public Subscription subscribe(String pattern) throws IOException {
        return subscribe(pattern, null, null);
    }

    /** Subscribes to {@code pattern} and delivers messages to {@code handler}. */
    public Subscription subscribe(String pattern, MessageHandler handler) throws IOException {
        return subscribe(pattern, null, Objects.requireNonNull(handler, "handler"));
    }

    /**
     * Subscribes to {@code pattern}. Within a queue group each message goes to
     * only one member; without one ({@code null}) every subscription receives a copy.
     *
     * @param handler receives messages on the subscription's thread, or
     *                {@code null} to read them with {@link Subscription#next()}
     */
    public Subscription subscribe(String pattern, String queueGroup, MessageHandler handler) throws IOException {
        Subjects.requirePattern(pattern);
        if (queueGroup != null && !isValidName(queueGroup)) {
            throw new IllegalArgumentException("queue group must use 1-96 ASCII letters, digits, '_', '-' or '.'");
        }
        Transport.Channel channel = open();
        try (Deadlines.Deadline deadline = deadlines.arm(channel, operationTimeout)) {
            try {
                Frames.write(channel.output(), new FrameBuilder(Protocol.SUBSCRIBE)
                        .string(pattern).string(queueGroup).toByteArray());
                Frames.read(channel.input()).expectOk();
            } catch (IOException e) {
                channel.abort();
                throw deadline.translate(e, "subscribe");
            }
        }
        Subscription[] self = new Subscription[1];
        Subscription subscription = new Subscription(pattern, queueGroup, channel, handler, subscriptionBuffer,
                () -> subscriptions.remove(self[0]));
        self[0] = subscription;
        subscriptions.add(subscription);
        if (closing) {
            subscription.fail(new RiftException("client closed"));
        } else {
            subscription.start();
        }
        return subscription;
    }

    /**
     * Sends a request and waits for the first reply. Rift creates a private
     * inbox for the reply, so no inbox subscribe grant is needed.
     *
     * @param timeout 1 ms to 120 seconds
     * @throws RiftRejectedException if there was no reply in time or no responder
     */
    public Message request(String subject, byte[] payload, Duration timeout) throws IOException {
        Subjects.requireLiteral(subject);
        Objects.requireNonNull(payload, "payload");
        long millis = timeout.toMillis();
        if (millis < 1 || millis > Protocol.MAX_REQUEST_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException("request timeout must be 1..120000 ms");
        }
        checkPayload(payload);
        byte[] header = new FrameBuilder(Protocol.REQUEST).u32(millis).string(subject).toByteArray();
        Transport.Channel channel = open();
        try (Deadlines.Deadline deadline = deadlines.arm(channel, timeout.plus(REQUEST_TRANSPORT_GRACE))) {
            try {
                Frames.write(channel.output(), header, payload);
                channel.finish();
                return Frames.read(channel.input()).expect(Protocol.MESSAGE).message();
            } catch (IOException e) {
                channel.abort();
                throw deadline.translate(e, "request to " + subject);
            }
        }
    }

    /** Sends a UTF-8 text request and waits for the first reply. */
    public Message request(String subject, String payload, Duration timeout) throws IOException {
        return request(subject, payload.getBytes(StandardCharsets.UTF_8), timeout);
    }

    /** Runs {@link #request(String, byte[], Duration)} on a background thread. */
    public CompletableFuture<Message> requestAsync(String subject, byte[] payload, Duration timeout) {
        return supplyAsync(() -> request(subject, payload, timeout));
    }

    /**
     * Answers the decisions Rift sends on {@code subject}, configured in
     * Rift's {@code extensions.decisions}. Joins the queue group
     * {@value #DEFAULT_DECIDER_GROUP}, so several deciders share the load.
     * Needs subscribe access to {@code subject} and publish access to
     * {@code _INBOX.>}.
     *
     * <pre>{@code
     * client.decide("relaxoban.login", request -> bans.activeBan(request.uuid())
     *         .thenApply(ban -> ban.map(b -> Decision.deny(b.message())).orElse(Decision.proceed())));
     * }</pre>
     *
     * @return the subscription; close it to stop answering
     */
    public Subscription decide(String subject, DecisionHandler handler) throws IOException {
        return decide(subject, DEFAULT_DECIDER_GROUP, handler);
    }

    /**
     * Answers decisions on {@code subject} within {@code queueGroup}, or as the
     * only decider with {@code null}. Two deciders outside a shared group both
     * answer and Rift uses whichever reply arrives first.
     */
    public Subscription decide(String subject, String queueGroup, DecisionHandler handler) throws IOException {
        Subjects.requireLiteral(subject);
        Objects.requireNonNull(handler, "handler");
        return subscribe(subject, queueGroup, message -> answer(message, handler));
    }

    private void answer(Message message, DecisionHandler handler) throws Exception {
        String reply = message.reply();
        if (reply == null) {
            return; // A plain publication, not a request.
        }
        DecisionRequest request = DecisionRequest.parse(message);
        CompletionStage<Decision> decision = handler.decide(request);
        Objects.requireNonNull(decision, "decision handler returned null");
        decision.whenComplete((answer, error) -> {
            if (error != null || answer == null) {
                LOGGER.log(System.Logger.Level.WARNING, "no decision for " + request.hook() + " of "
                        + request.name() + " on " + message.subject() + "; Rift applies its timeout policy", error);
                return;
            }
            byte[] payload = answer.toJson().getBytes(StandardCharsets.UTF_8);
            try {
                // Publishing waits for the broker, so keep it off the caller's thread.
                asyncExecutor.execute(() -> {
                    try {
                        publish(reply, payload);
                    } catch (IOException e) {
                        LOGGER.log(System.Logger.Level.WARNING, "could not reply to a decision on "
                                + message.subject(), e);
                    }
                });
            } catch (RejectedExecutionException e) {
                // The client closed; Rift applies its timeout policy.
            }
        });
    }

    /**
     * Appends a record to a retained stream and waits for the stream's
     * durability boundary. This does not publish to live subscribers.
     */
    public StoredRecord streamPublish(String stream, String subject, byte[] payload) throws IOException {
        return streamPublish(stream, subject, null, payload);
    }

    /** Appends a record with a reply subject to a retained stream. */
    public StoredRecord streamPublish(String stream, String subject, String reply, byte[] payload)
            throws IOException {
        Subjects.requireLiteral(subject);
        if (reply != null) {
            Subjects.requireLiteral(reply);
        }
        checkPayload(payload);
        byte[] header = new FrameBuilder(Protocol.STREAM_PUBLISH)
                .string(stream).string(subject).string(reply).toByteArray();
        Frame response = command(header, payload, "stream publish").expect(Protocol.STORED);
        StoredRecord record = new StoredRecord(response.u64(), response.u64());
        response.end();
        return record;
    }

    /**
     * Creates or reopens a durable consumer owned by this principal. Reopening
     * with the same name and settings resumes where the consumer left off.
     */
    public StreamConsumer consumer(String stream, String name, ConsumerConfig config) throws IOException {
        requireName(stream, "stream name");
        requireName(name, "consumer name");
        byte[] frame = new FrameBuilder(Protocol.CONSUMER_OPEN)
                .string(stream)
                .string(name)
                .string(config.filterSubject())
                .u64(config.startSequence())
                .u32(config.ackWait().toMillis())
                .u32(config.maxAckPending())
                .toByteArray();
        command(frame, new byte[0], "consumer open").expectOk();
        return new RemoteConsumer(stream, name);
    }

    /** Typed access to Rift's {@code rift.control.*} subjects. */
    public RiftControl control() {
        return control;
    }

    /** Whether the connection is still open. */
    public boolean isConnected() {
        return !closing && transport.isOpen();
    }

    /** Completes when the connection closes, whether by {@link #close()} or a failure. */
    public CompletableFuture<Void> closed() {
        return closed.copy();
    }

    /** Closes the connection, ending all subscriptions and publishers. */
    @Override
    public void close() {
        if (closing) {
            return;
        }
        closing = true;
        transport.close();
        connectionClosed();
    }

    private void connectionClosed() {
        closing = true;
        RiftException error = new RiftException("connection to Rift closed");
        for (Subscription subscription : List.copyOf(subscriptions)) {
            subscription.fail(error);
        }
        for (Publisher publisher : List.copyOf(publishers)) {
            publisher.abort();
        }
        publishers.clear();
        deadlines.close();
        asyncExecutor.shutdownNow();
        closed.complete(null);
    }

    private <T> CompletableFuture<T> supplyAsync(IoSupplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            asyncExecutor.execute(() -> {
                try {
                    future.complete(supplier.get());
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (RuntimeException e) {
            future.completeExceptionally(new RiftException("client closed", e));
        }
        return future;
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }

    private Publisher openPublisher(boolean tracked) throws IOException {
        Transport.Channel channel = open();
        Publisher[] self = new Publisher[1];
        Publisher publisher = new Publisher(channel, deadlines, operationTimeout, () -> publishers.remove(self[0]));
        self[0] = publisher;
        if (tracked) {
            publishers.add(publisher);
        }
        return publisher;
    }

    private Frame command(byte[] header, byte[] payload, String operation) throws IOException {
        Transport.Channel channel = open();
        try (Deadlines.Deadline deadline = deadlines.arm(channel, operationTimeout)) {
            try {
                Frames.write(channel.output(), header, payload);
                channel.finish();
                return Frames.read(channel.input());
            } catch (IOException e) {
                channel.abort();
                throw deadline.translate(e, operation);
            }
        }
    }

    private Transport.Channel open() throws IOException {
        if (closing) {
            throw new RiftException("client closed");
        }
        try {
            return transport.open();
        } catch (IOException e) {
            throw new RiftException("could not open a stream: " + e.getMessage(), e);
        }
    }

    private static void checkPayload(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload.length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload exceeds 1 MiB transport limit");
        }
    }

    static boolean isValidName(String name) {
        if (name == null || name.isEmpty() || name.length() > 96) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.';
            if (!allowed) {
                return false;
            }
        }
        return true;
    }

    private static void requireName(String name, String what) {
        if (!isValidName(name)) {
            throw new IllegalArgumentException(what + " must use 1-96 ASCII letters, digits, '_', '-' or '.'");
        }
    }

    private final class RemoteConsumer implements StreamConsumer {
        private final String stream;
        private final String name;

        private RemoteConsumer(String stream, String name) {
            this.stream = stream;
            this.name = name;
        }

        @Override
        public String stream() {
            return stream;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<Delivery> fetch(int limit) throws IOException {
            if (limit < 1 || limit > Protocol.MAX_FETCH) {
                throw new IllegalArgumentException("fetch limit must be 1..256");
            }
            byte[] frame = new FrameBuilder(Protocol.CONSUMER_FETCH)
                    .string(stream).string(name).u32(limit).toByteArray();
            Transport.Channel channel = open();
            try (Deadlines.Deadline deadline = deadlines.arm(channel, operationTimeout)) {
                try {
                    Frames.write(channel.output(), frame);
                    channel.finish();
                    List<Delivery> deliveries = new ArrayList<>();
                    while (true) {
                        Frame response = Frames.read(channel.input());
                        if (response.opcode() == Protocol.OK) {
                            response.expectOk();
                            return deliveries;
                        }
                        if (deliveries.size() >= limit) {
                            throw new RiftException("server exceeded fetch limit");
                        }
                        response.expect(Protocol.DELIVERY);
                        long sequence = response.u64();
                        long timestamp = response.u64();
                        long count = response.u32();
                        deliveries.add(new Delivery(sequence, timestamp, count, response.message()));
                    }
                } catch (IOException e) {
                    channel.abort();
                    throw deadline.translate(e, "fetch");
                }
            }
        }

        @Override
        public void ack(long sequence) throws IOException {
            byte[] frame = new FrameBuilder(Protocol.ACK)
                    .string(stream).string(name).u64(sequence).toByteArray();
            command(frame, new byte[0], "ack").expectOk();
        }

        @Override
        public String toString() {
            return "StreamConsumer[stream=" + stream + ", name=" + name + "]";
        }
    }

    /** Builder for {@link RiftClient}. */
    public static final class Builder {
        private InetSocketAddress address;
        private String serverName;
        private String token;
        private X509TrustManager trustManager;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration operationTimeout = DEFAULT_OPERATION_TIMEOUT;
        private int subscriptionBuffer = DEFAULT_SUBSCRIPTION_BUFFER;

        private Builder() {
        }

        /** The UDP address of Rift's messaging listener. */
        public Builder address(String host, int port) {
            this.address = InetSocketAddress.createUnresolved(host, port);
            return this;
        }

        public Builder address(InetSocketAddress address) {
            this.address = Objects.requireNonNull(address, "address");
            return this;
        }

        /**
         * The DNS name the server certificate is verified against. Defaults to
         * the address host. Certificates must carry this name as a DNS subject
         * alternative name; IP address entries are not supported.
         */
        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        /** The principal's token as configured through its {@code token_env} in Rift. */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Reads the token from an environment variable.
         *
         * @throws IllegalStateException if the variable is not set
         */
        public Builder tokenFromEnvironment(String variable) {
            String value = System.getenv(variable);
            if (value == null || value.isEmpty()) {
                throw new IllegalStateException("environment variable " + variable + " is not set");
            }
            return token(value);
        }

        /** Trusts the CA or server certificates in a PEM file. */
        public Builder trustCertificates(Path pemFile) throws IOException {
            return trustCertificates(TrustManagers.readCertificates(pemFile));
        }

        /** Trusts the given CA or server certificates. */
        public Builder trustCertificates(Collection<X509Certificate> certificates) throws IOException {
            this.trustManager = TrustManagers.trusting(certificates);
            return this;
        }

        /** Uses a custom trust manager; the server name is still verified. */
        public Builder trustManager(X509TrustManager trustManager) {
            this.trustManager = Objects.requireNonNull(trustManager, "trustManager");
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = requirePositive(connectTimeout, "connectTimeout");
            return this;
        }

        /** Deadline for individual operations other than requests. Defaults to 30 seconds. */
        public Builder operationTimeout(Duration operationTimeout) {
            this.operationTimeout = requirePositive(operationTimeout, "operationTimeout");
            return this;
        }

        /** How many unread messages a pull subscription buffers before applying backpressure. */
        public Builder subscriptionBuffer(int messages) {
            if (messages < 1) {
                throw new IllegalArgumentException("subscription buffer must be positive");
            }
            this.subscriptionBuffer = messages;
            return this;
        }

        /** Connects and authenticates. Blocks until the connection is ready. */
        public RiftClient connect() throws IOException {
            if (address == null) {
                throw new IllegalStateException("address is required");
            }
            if (token == null || token.isEmpty()) {
                throw new IllegalStateException("token is required");
            }
            if (trustManager == null) {
                throw new IllegalStateException("trusted certificates are required");
            }
            String name = serverName != null ? serverName : address.getHostString();
            Transport transport;
            try {
                transport = QuicTransport.connect(address, name, trustManager, connectTimeout);
            } catch (RiftException e) {
                throw e;
            } catch (IOException e) {
                throw new RiftException("could not connect to Rift at " + address.getHostString() + ":"
                        + address.getPort() + ": " + e.getMessage(), e);
            }
            return authenticate(transport, token, operationTimeout, subscriptionBuffer);
        }

        /** Connects on a background thread. */
        public CompletableFuture<RiftClient> connectAsync() {
            CompletableFuture<RiftClient> future = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                try {
                    future.complete(connect());
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            }, "rift-connect");
            thread.setDaemon(true);
            thread.start();
            return future;
        }

        private static Duration requirePositive(Duration duration, String name) {
            if (duration == null || duration.isNegative() || duration.isZero()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return duration;
        }
    }
}
