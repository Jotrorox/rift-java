package com.jotrorox.rift.testing;

import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftClient;
import com.jotrorox.rift.Subjects;
import com.jotrorox.rift.internal.Frame;
import com.jotrorox.rift.internal.FrameBuilder;
import com.jotrorox.rift.internal.Frames;
import com.jotrorox.rift.internal.Protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A small in-memory implementation of the server side of the Rift messaging
 * protocol. It models the behavior clients rely on, not Rift's internals.
 */
public final class FakeRift {
    public static final String TOKEN = "fake-token-0123456789abcdefghijklmnopqrstuvw";

    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final List<String> denied = new CopyOnWriteArrayList<>();
    private final List<Message> published = new CopyOnWriteArrayList<>();
    private final Map<String, List<Message>> streams = new ConcurrentHashMap<>();
    private final Map<String, Long> consumers = new ConcurrentHashMap<>();
    private final AtomicLong inboxes = new AtomicLong();
    private final List<MemoryTransport> transports = new CopyOnWriteArrayList<>();
    private volatile boolean silent;
    private volatile boolean offline;

    /** Rejects publications and subscriptions to subjects starting with {@code prefix}. */
    public FakeRift deny(String prefix) {
        denied.add(prefix);
        return this;
    }

    /** Declares a retained stream. */
    public FakeRift stream(String name) {
        streams.put(name, new CopyOnWriteArrayList<>());
        return this;
    }

    /** While silent, the server reads commands but never answers them. */
    public void silent(boolean silent) {
        this.silent = silent;
    }

    /** Every message the broker accepted, in order. */
    public List<Message> published() {
        return published;
    }

    public RiftClient connect() throws IOException {
        return connect(Duration.ofSeconds(5));
    }

    public RiftClient connect(Duration operationTimeout) throws IOException {
        return RiftClient.over(transport(), TOKEN, operationTimeout);
    }

    public MemoryTransport transport() throws IOException {
        if (offline) {
            throw new IOException("connection refused");
        }
        MemoryTransport transport = new MemoryTransport(this::serve);
        transports.add(transport);
        return transport;
    }

    /** While offline, new connections are refused. */
    public void offline(boolean offline) {
        this.offline = offline;
    }

    /** Drops every open connection, like a Rift restart. */
    public void disconnectAll() {
        for (MemoryTransport transport : transports) {
            transport.close();
        }
        transports.clear();
    }

    /** Number of live subscriptions whose pattern equals {@code pattern}. */
    public long subscriptions(String pattern) {
        return subscribers.stream().filter(subscriber -> subscriber.pattern.equals(pattern)
                && subscriber.out != null).count();
    }

    private void serve(MemoryTransport.ServerSide side) {
        boolean first = true;
        while (true) {
            Frame frame;
            try {
                frame = Frames.read(side.input());
            } catch (IOException e) {
                return;
            }
            if (silent) {
                continue;
            }
            try {
                if (!handle(frame, side, first)) {
                    side.finish();
                    return;
                }
            } catch (Denied e) {
                error(side.output(), Protocol.ERROR_PERMISSION, e.getMessage());
                side.finish();
                side.fromClient().reset();
                return;
            } catch (IOException | IllegalArgumentException e) {
                error(side.output(), Protocol.ERROR_OPERATION, e.getMessage());
                side.finish();
                side.fromClient().reset();
                return;
            }
            first = false;
        }
    }

    /** Returns {@code false} once the stream is done. */
    private boolean handle(Frame frame, MemoryTransport.ServerSide side, boolean first) throws IOException {
        int opcode = frame.u8();
        OutputStream out = side.output();
        switch (opcode) {
            case Protocol.AUTH -> {
                String token = frame.string();
                if (!first || !token.equals(TOKEN)) {
                    throw new Denied("authentication failed");
                }
                Frames.writeSimple(out, Protocol.OK);
                return false;
            }
            case Protocol.PUBLISH, Protocol.PUBLISH_ACK -> {
                Message message = frame.message();
                checkAllowed(message.subject());
                route(message);
                if (opcode == Protocol.PUBLISH_ACK) {
                    Frames.writeSimple(out, Protocol.OK);
                }
                return true;
            }
            case Protocol.PING -> {
                frame.end();
                Frames.writeSimple(out, Protocol.OK);
                return true;
            }
            case Protocol.SUBSCRIBE -> {
                String pattern = frame.string();
                String queue = frame.string();
                frame.end();
                checkAllowed(pattern);
                Subscriber subscriber = new Subscriber(pattern, queue, out);
                subscribers.add(subscriber);
                Frames.writeSimple(out, Protocol.OK);
                try {
                    // Hold the stream until the client cancels it.
                    while (side.input().read() >= 0) {
                        // Discard.
                    }
                } catch (IOException ignored) {
                    // Cancelled.
                } finally {
                    subscribers.remove(subscriber);
                }
                return false;
            }
            case Protocol.REQUEST -> {
                long wait = frame.u32();
                String subject = frame.string();
                byte[] payload = frame.rest();
                checkAllowed(subject);
                String inbox = "_INBOX.fake." + inboxes.incrementAndGet();
                LinkedBlockingQueue<Message> replies = new LinkedBlockingQueue<>();
                Subscriber reply = new Subscriber(inbox, "", null, replies);
                subscribers.add(reply);
                try {
                    if (route(new Message(subject, inbox, payload)) == 0) {
                        throw new IOException("no responders");
                    }
                    Message answer = replies.poll(wait, TimeUnit.MILLISECONDS);
                    if (answer == null) {
                        throw new IOException("request timed out");
                    }
                    writeMessage(out, Protocol.MESSAGE, answer, new byte[0]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } finally {
                    subscribers.remove(reply);
                }
                return false;
            }
            case Protocol.STREAM_PUBLISH -> {
                String stream = frame.string();
                Message message = frame.message();
                checkAllowed(message.subject());
                List<Message> records = requireStream(stream);
                records.add(message);
                byte[] stored = new FrameBuilder(Protocol.STORED).u64(records.size()).u64(1_700_000_000_000L)
                        .toByteArray();
                Frames.write(out, stored);
                return false;
            }
            case Protocol.CONSUMER_OPEN -> {
                String stream = frame.string();
                String name = frame.string();
                frame.string();
                long start = frame.u64();
                frame.u32();
                frame.u32();
                frame.end();
                requireStream(stream);
                consumers.putIfAbsent(stream + "/" + name, start);
                Frames.writeSimple(out, Protocol.OK);
                return false;
            }
            case Protocol.CONSUMER_FETCH -> {
                String stream = frame.string();
                String name = frame.string();
                long limit = frame.u32();
                List<Message> records = requireStream(stream);
                Long next = consumers.get(stream + "/" + name);
                if (next == null) {
                    throw new IOException("unknown consumer");
                }
                for (long sequence = next; sequence <= records.size() && sequence < next + limit; sequence++) {
                    byte[] prefix = new FrameBuilder(0).u64(sequence).u64(1_700_000_000_000L).u32(1).toByteArray();
                    writeMessage(out, Protocol.DELIVERY, records.get((int) sequence - 1),
                            java.util.Arrays.copyOfRange(prefix, 1, prefix.length));
                }
                Frames.writeSimple(out, Protocol.OK);
                return false;
            }
            case Protocol.ACK -> {
                String stream = frame.string();
                String name = frame.string();
                long sequence = frame.u64();
                consumers.computeIfPresent(stream + "/" + name, (key, next) -> Math.max(next, sequence + 1));
                Frames.writeSimple(out, Protocol.OK);
                return false;
            }
            default -> throw new IOException("unknown opcode");
        }
    }

    private int route(Message message) {
        published.add(message);
        int delivered = 0;
        List<String> groups = new ArrayList<>();
        for (Subscriber subscriber : subscribers) {
            if (!Subjects.matches(subscriber.pattern, message.subject())) {
                continue;
            }
            if (!subscriber.queue.isEmpty()) {
                if (groups.contains(subscriber.queue)) {
                    continue;
                }
                groups.add(subscriber.queue);
            }
            if (subscriber.deliver(message)) {
                delivered++;
            }
        }
        return delivered;
    }

    private void checkAllowed(String subject) throws Denied {
        for (String prefix : denied) {
            if (subject.startsWith(prefix)) {
                throw new Denied("publication not permitted");
            }
        }
    }

    private List<Message> requireStream(String name) throws IOException {
        List<Message> records = streams.get(name);
        if (records == null) {
            throw new IOException("unknown stream");
        }
        return records;
    }

    private static void writeMessage(OutputStream out, int opcode, Message message, byte[] extra) throws IOException {
        byte[] head = new FrameBuilder(opcode).toByteArray();
        byte[] fields = new FrameBuilder(0).string(message.subject()).string(message.reply()).toByteArray();
        byte[] header = new byte[head.length + extra.length + fields.length - 1];
        System.arraycopy(head, 0, header, 0, head.length);
        System.arraycopy(extra, 0, header, head.length, extra.length);
        System.arraycopy(fields, 1, header, head.length + extra.length, fields.length - 1);
        synchronized (out) {
            Frames.write(out, header, message.payload());
        }
    }

    private static void error(OutputStream out, int code, String message) {
        try {
            Frames.write(out, new FrameBuilder(Protocol.ERROR).u16(code)
                    .string(message == null ? "error" : message).toByteArray());
        } catch (IOException ignored) {
            // Client went away.
        }
    }

    private static final class Denied extends IOException {
        private static final long serialVersionUID = 1L;

        Denied(String message) {
            super(message);
        }
    }

    private static final class Subscriber {
        final String pattern;
        final String queue;
        final OutputStream out;
        final LinkedBlockingQueue<Message> local;

        Subscriber(String pattern, String queue, OutputStream out) {
            this(pattern, queue, out, null);
        }

        Subscriber(String pattern, String queue, OutputStream out, LinkedBlockingQueue<Message> local) {
            this.pattern = pattern;
            this.queue = queue;
            this.out = out;
            this.local = local;
        }

        boolean deliver(Message message) {
            if (local != null) {
                return local.offer(message);
            }
            try {
                writeMessage(out, Protocol.MESSAGE, message, new byte[0]);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }
}
