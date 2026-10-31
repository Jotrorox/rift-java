package com.jotrorox.rift.testing;

import com.jotrorox.rift.internal.Transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** An in-memory {@link Transport}; every opened channel is handed to a server-side handler. */
public final class MemoryTransport implements Transport {
    private final Consumer<ServerSide> server;
    private final List<Runnable> closeCallbacks = new CopyOnWriteArrayList<>();
    private final List<Pipe> pipes = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public MemoryTransport(Consumer<ServerSide> server) {
        this.server = server;
    }

    @Override
    public Channel open() throws IOException {
        if (closed.get()) {
            throw new IOException("connection closed");
        }
        Pipe toServer = new Pipe();
        Pipe toClient = new Pipe();
        pipes.add(toServer);
        pipes.add(toClient);
        ServerSide side = new ServerSide(toServer.in, toClient.out, toServer, toClient);
        Thread thread = new Thread(() -> server.accept(side), "memory-server");
        thread.setDaemon(true);
        thread.start();
        return new Channel() {
            @Override
            public InputStream input() {
                return toClient.in;
            }

            @Override
            public OutputStream output() {
                return toServer.out;
            }

            @Override
            public void finish() {
                toServer.finish();
            }

            @Override
            public void abort() {
                toServer.reset();
                toClient.reset();
            }
        };
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public void onClose(Runnable callback) {
        closeCallbacks.add(callback);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Pipe pipe : pipes) {
                pipe.reset();
            }
            closeCallbacks.forEach(Runnable::run);
        }
    }

    /** The server's view of one stream. */
    public record ServerSide(InputStream input, OutputStream output, Pipe fromClient, Pipe toClient) {
        /** Finishes the server's sending half. */
        public void finish() {
            toClient.finish();
        }
    }

    /** A one-directional byte pipe that supports FIN and reset. */
    public static final class Pipe {
        private final byte[] buffer = new byte[4 * 1024 * 1024];
        private int head;
        private int size;
        private boolean finished;
        private boolean reset;

        final InputStream in = new InputStream() {
            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] target, int offset, int length) throws IOException {
                if (length == 0) {
                    return 0;
                }
                synchronized (Pipe.this) {
                    while (size == 0 && !finished && !reset) {
                        await();
                    }
                    if (reset) {
                        throw new IOException("stream reset");
                    }
                    if (size == 0) {
                        return -1;
                    }
                    int count = Math.min(length, size);
                    for (int i = 0; i < count; i++) {
                        target[offset + i] = buffer[(head + i) % buffer.length];
                    }
                    head = (head + count) % buffer.length;
                    size -= count;
                    Pipe.this.notifyAll();
                    return count;
                }
            }
        };

        final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] source, int offset, int length) throws IOException {
                synchronized (Pipe.this) {
                    for (int i = 0; i < length; i++) {
                        while (size == buffer.length && !reset) {
                            await();
                        }
                        if (reset || finished) {
                            throw new IOException("output stream is reset");
                        }
                        buffer[(head + size) % buffer.length] = source[offset + i];
                        size++;
                    }
                    Pipe.this.notifyAll();
                }
            }

            @Override
            public void close() {
                finish();
            }
        };

        public synchronized void finish() {
            finished = true;
            notifyAll();
        }

        public synchronized void reset() {
            reset = true;
            notifyAll();
        }

        private void await() throws InterruptedIOException {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }
    }
}
