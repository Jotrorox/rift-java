package com.jotrorox.rift;

import com.jotrorox.rift.internal.Frames;
import com.jotrorox.rift.internal.MessageBuffer;
import com.jotrorox.rift.internal.Protocol;
import com.jotrorox.rift.internal.Transport;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A live subscription on its own QUIC stream.
 *
 * <p>Subscriptions created without a handler buffer incoming messages for
 * {@link #next()} and {@link #poll(Duration)}. Subscriptions created with a
 * {@link MessageHandler} deliver to the handler instead; their pull methods
 * throw {@link IllegalStateException}.
 *
 * <p>Live delivery is at most once: messages published while no subscription
 * matches are discarded, and a subscriber that falls too far behind is
 * disconnected by Rift. Watch {@link #closed()} to resubscribe, and use
 * retained streams for work that must not be lost.
 */
public final class Subscription implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(Subscription.class.getName());

    private final String pattern;
    private final String queueGroup;
    private final Transport.Channel channel;
    private final MessageHandler handler;
    private final MessageBuffer buffer;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final Runnable onClose;
    private volatile Thread reader;

    Subscription(String pattern, String queueGroup, Transport.Channel channel, MessageHandler handler,
                 int bufferSize, Runnable onClose) {
        this.pattern = pattern;
        this.queueGroup = queueGroup;
        this.channel = channel;
        this.handler = handler;
        this.buffer = handler == null ? new MessageBuffer(bufferSize) : null;
        this.onClose = onClose;
    }

    void start() {
        Thread thread = new Thread(this::run, "rift-subscription-" + pattern);
        thread.setDaemon(true);
        reader = thread;
        thread.start();
    }

    /** The subscribed subject pattern. */
    public String pattern() {
        return pattern;
    }

    /** The queue group, or {@code null} when every matching subscription receives a copy. */
    public String queueGroup() {
        return queueGroup;
    }

    /**
     * Blocks until the next message arrives.
     *
     * @throws RiftException if the subscription ended; the cause describes why
     */
    public Message next() throws IOException, InterruptedException {
        Message message = requireBuffer().take(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        if (message == null) {
            throw ended();
        }
        return message;
    }

    /**
     * Waits up to {@code timeout} for the next message.
     *
     * @return the message, or {@code null} if none arrived in time
     * @throws RiftException if the subscription ended and no buffered messages remain
     */
    public Message poll(Duration timeout) throws IOException, InterruptedException {
        MessageBuffer buffer = requireBuffer();
        Message message = buffer.take(timeout.toNanos(), TimeUnit.NANOSECONDS);
        if (message == null && buffer.isDrained()) {
            throw ended();
        }
        return message;
    }

    /**
     * Completes when the subscription ends: normally after {@link #close()},
     * exceptionally when the server or connection ended it.
     */
    public CompletableFuture<Void> closed() {
        return closed.copy();
    }

    /** Whether the subscription is still receiving messages. */
    public boolean isActive() {
        return !closed.isDone();
    }

    /** Cancels the subscription and discards messages that were not read yet. */
    @Override
    public void close() {
        channel.abort();
        if (finish(null)) {
            Thread thread = reader;
            if (thread != null && thread != Thread.currentThread()) {
                thread.interrupt();
            }
        }
    }

    /** Ends the subscription because its connection failed. */
    void fail(IOException error) {
        channel.abort();
        finish(error);
    }

    private void run() {
        try {
            while (!closed.isDone()) {
                Message message = Frames.read(channel.input()).expect(Protocol.MESSAGE).message();
                if (handler != null) {
                    dispatch(message);
                } else if (!buffer.put(message)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            channel.abort();
            finish(e);
        }
    }

    private void dispatch(Message message) {
        try {
            handler.onMessage(message);
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.WARNING, "message handler for " + pattern + " failed", e);
        }
    }

    private boolean finish(IOException error) {
        boolean first = error == null ? closed.complete(null) : closed.completeExceptionally(error);
        if (first) {
            if (buffer != null) {
                buffer.end(error == null);
            }
            onClose.run();
        }
        return first;
    }

    private MessageBuffer requireBuffer() {
        if (buffer == null) {
            throw new IllegalStateException("subscription delivers to a handler");
        }
        return buffer;
    }

    private RiftException ended() {
        Throwable cause = closed.isCompletedExceptionally() ? closed.exceptionNow() : null;
        return new RiftException("subscription to " + pattern + " ended", cause);
    }
}
