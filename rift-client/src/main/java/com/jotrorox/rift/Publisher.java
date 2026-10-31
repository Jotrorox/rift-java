package com.jotrorox.rift;

import com.jotrorox.rift.internal.Deadlines;
import com.jotrorox.rift.internal.Frame;
import com.jotrorox.rift.internal.FrameBuilder;
import com.jotrorox.rift.internal.Frames;
import com.jotrorox.rift.internal.Protocol;
import com.jotrorox.rift.internal.Transport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An ordered publisher on its own QUIC stream.
 *
 * <p>{@link #publish(String, byte[])} pipelines messages without waiting for
 * the broker, which gives the highest throughput. Errors such as a denied
 * subject surface on the next {@link #flush()} or {@link #publishConfirmed}
 * call. {@code flush} is a barrier: once it returns, every earlier publication
 * on this publisher was accepted by the broker. Neither call means that a
 * subscriber has processed the message.
 *
 * <p>After any failure the publisher becomes unusable; create a new one to
 * continue. A failed publication may already have reached the broker, so
 * retrying can duplicate messages. Publishers are thread-safe; messages from
 * one publisher keep their order.
 */
public final class Publisher implements AutoCloseable {
    private final Transport.Channel channel;
    private final Deadlines deadlines;
    private final Duration timeout;
    private final Runnable onClose;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean healthy = true;
    private boolean closed;

    Publisher(Transport.Channel channel, Deadlines deadlines, Duration timeout, Runnable onClose) {
        this.channel = channel;
        this.deadlines = deadlines;
        this.timeout = timeout;
        this.onClose = onClose;
    }

    /** Queues a publication without waiting for broker acceptance. */
    public void publish(String subject, byte[] payload) throws IOException {
        write(Protocol.PUBLISH, subject, null, payload);
    }

    /** Queues a UTF-8 text publication without waiting for broker acceptance. */
    public void publish(String subject, String payload) throws IOException {
        publish(subject, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Queues a publication with a reply subject. Rift requires reply subjects
     * to be literal {@code _INBOX.} subjects covered by this principal's
     * subscribe grants.
     */
    public void publish(String subject, String reply, byte[] payload) throws IOException {
        write(Protocol.PUBLISH, subject, reply, payload);
    }

    /** Publishes and waits until the broker accepted the message. */
    public void publishConfirmed(String subject, byte[] payload) throws IOException {
        write(Protocol.PUBLISH_ACK, subject, null, payload);
    }

    /** Publishes with a reply subject and waits until the broker accepted the message. */
    public void publishConfirmed(String subject, String reply, byte[] payload) throws IOException {
        write(Protocol.PUBLISH_ACK, subject, reply, payload);
    }

    /** Waits until all earlier publications on this publisher were accepted by the broker. */
    public void flush() throws IOException {
        lock.lock();
        try {
            ensureUsable();
            healthy = false;
            try (Deadlines.Deadline deadline = deadlines.arm(channel, timeout)) {
                try {
                    writeOrReportServerError(new byte[] {Protocol.PING}, new byte[0]);
                    Frames.read(channel.input()).expectOk();
                } catch (IOException e) {
                    throw deadline.translate(e, "flush");
                }
            }
            healthy = true;
        } finally {
            lock.unlock();
        }
    }

    /** Whether this publisher can still be used. */
    public boolean isUsable() {
        lock.lock();
        try {
            return healthy && !closed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the stream. Publications that were queued but not flushed are
     * still delivered to the broker unless the connection closes first.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            channel.finish();
        } finally {
            lock.unlock();
        }
        onClose.run();
    }

    void abort() {
        lock.lock();
        try {
            closed = true;
            channel.abort();
        } finally {
            lock.unlock();
        }
    }

    private void write(int opcode, String subject, String reply, byte[] payload) throws IOException {
        Subjects.requireLiteral(subject);
        if (reply != null) {
            Subjects.requireLiteral(reply);
        }
        Objects.requireNonNull(payload, "payload");
        if (payload.length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload exceeds 1 MiB transport limit");
        }
        byte[] header = new FrameBuilder(opcode).string(subject).string(reply).toByteArray();
        lock.lock();
        try {
            ensureUsable();
            healthy = false;
            try (Deadlines.Deadline deadline = deadlines.arm(channel, timeout)) {
                try {
                    writeOrReportServerError(header, payload);
                    if (opcode == Protocol.PUBLISH_ACK) {
                        Frame response = Frames.read(channel.input());
                        response.expectOk();
                    }
                } catch (IOException e) {
                    throw deadline.translate(e, "publish");
                }
            }
            healthy = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rift answers a rejected pipelined publication with ERROR and closes the
     * stream, so a later write can fail before that reply was read. Report the
     * server's reason instead of the bare stream reset.
     */
    private void writeOrReportServerError(byte[] header, byte[] payload) throws IOException {
        try {
            Frames.write(channel.output(), header, payload);
        } catch (IOException writeFailure) {
            Frame pending;
            try {
                pending = Frames.read(channel.input());
            } catch (IOException readFailure) {
                writeFailure.addSuppressed(readFailure);
                throw writeFailure;
            }
            pending.expect(Protocol.ERROR);
            throw writeFailure;
        }
    }

    private void ensureUsable() throws RiftException {
        if (closed) {
            throw new RiftException("publisher is closed");
        }
        if (!healthy) {
            throw new RiftException("publisher failed earlier; create a new publisher");
        }
    }
}
