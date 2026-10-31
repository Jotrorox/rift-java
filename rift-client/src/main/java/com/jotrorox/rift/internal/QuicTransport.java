package com.jotrorox.rift.internal;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.QuicStream;

import javax.net.ssl.X509TrustManager;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** {@link Transport} over a QUIC connection with ALPN {@code rift-messaging/1}. */
public final class QuicTransport implements Transport {
    private static final long STREAM_RECEIVE_BUFFER = 2L * Protocol.MAX_FRAME_BYTES;
    private static final int HANDSHAKE_ATTEMPTS = 3;
    private static final String HANDSHAKE_ERROR = "Handshake error:";

    private final QuicClientConnection connection;
    private final List<Runnable> closeCallbacks = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private QuicTransport(QuicClientConnection connection) {
        this.connection = connection;
    }

    /**
     * Connects to {@code address} and verifies the server certificate for {@code serverName}.
     * The address may differ from the server name, for example when dialing an IP address.
     */
    public static QuicTransport connect(InetSocketAddress address, String serverName,
                                        X509TrustManager trustManager, Duration connectTimeout) throws IOException {
        for (int attempt = 1; ; attempt++) {
            QuicClientConnection connection = newConnection(address, serverName, trustManager, connectTimeout);
            QuicTransport transport = new QuicTransport(connection);
            connection.setConnectionListener(event -> transport.terminated());
            try {
                connection.connect();
                return transport;
            } catch (ConnectException e) {
                if (attempt >= HANDSHAKE_ATTEMPTS || !isHandshakeRace(e)) {
                    throw e;
                }
            }
        }
    }

    private static QuicClientConnection newConnection(InetSocketAddress address, String serverName,
                                                      X509TrustManager trustManager, Duration connectTimeout)
            throws IOException {
        QuicClientConnection.Builder builder = QuicClientConnection.newBuilder()
                .host(serverName)
                .port(address.getPort())
                .applicationProtocol(Protocol.ALPN)
                .connectTimeout(connectTimeout)
                .maxIdleTimeout(Duration.ofSeconds(60))
                .defaultStreamReceiveBufferSize(STREAM_RECEIVE_BUFFER)
                .maxOpenPeerInitiatedBidirectionalStreams(0)
                .maxOpenPeerInitiatedUnidirectionalStreams(0)
                .customTrustManager(trustManager);
        String host = address.isUnresolved() ? address.getHostString() : address.getAddress().getHostAddress();
        if (!host.equalsIgnoreCase(serverName)) {
            // Kwik dials the "proxy" host while still using serverName for SNI and verification.
            builder.proxy(host);
        }
        return builder.build();
    }

    /**
     * Whether a failed handshake hit the race in Kwik's TLS engine, which sends the ClientHello before it
     * starts waiting for the ServerHello. A fast server (typically on loopback) can answer in between, and
     * the engine then rejects the ServerHello, sometimes without recording an error. Nothing has been sent
     * on the connection yet, so a fresh one is safe to try.
     */
    static boolean isHandshakeRace(ConnectException e) {
        String message = e.getMessage();
        if (message == null || !message.startsWith(HANDSHAKE_ERROR)) {
            return false;
        }
        String reason = message.substring(HANDSHAKE_ERROR.length()).strip();
        return reason.isEmpty() || reason.contains("unexpected server hello");
    }

    @Override
    public Channel open() throws IOException {
        QuicStream stream = connection.createStream(true);
        return new QuicChannel(stream, new SendStream(stream.getOutputStream()));
    }

    @Override
    public boolean isOpen() {
        return !closed.get() && connection.isConnected();
    }

    @Override
    public void onClose(Runnable callback) {
        closeCallbacks.add(callback);
        if (closed.get()) {
            callback.run();
        }
    }

    @Override
    public void close() {
        if (!closed.get()) {
            connection.close(0, "client closed");
        }
        terminated();
    }

    private void terminated() {
        if (closed.compareAndSet(false, true)) {
            for (Runnable callback : closeCallbacks) {
                callback.run();
            }
        }
    }

    private record QuicChannel(QuicStream stream, OutputStream output) implements Channel {
        @Override
        public InputStream input() {
            return stream.getInputStream();
        }

        @Override
        public void finish() {
            try {
                stream.getOutputStream().close();
            } catch (IOException ignored) {
                // The stream is already closed or reset.
            }
        }

        @Override
        public void abort() {
            try {
                stream.resetStream(0);
            } catch (RuntimeException ignored) {
                // Already finished or reset.
            }
            try {
                stream.abortReading(0);
            } catch (RuntimeException ignored) {
                // Already fully read.
            }
        }
    }

    /**
     * Kwik sends written data right away and its {@code flush} only checks the stream state. Rift stops
     * reading a request stream once it has the whole request, so a STOP_SENDING arriving between a write
     * and its flush would fail a request the server already answered. A reset still fails the next write.
     */
    private static final class SendStream extends FilterOutputStream {
        private SendStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() {
        }
    }
}
