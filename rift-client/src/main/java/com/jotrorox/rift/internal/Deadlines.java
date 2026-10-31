package com.jotrorox.rift.internal;

import com.jotrorox.rift.RiftTimeoutException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enforces operation deadlines on blocking stream I/O by aborting the stream
 * when time runs out, which unblocks any pending read or write.
 */
public final class Deadlines implements AutoCloseable {
    private final ScheduledExecutorService timer;

    public Deadlines() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "rift-deadlines");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        this.timer = executor;
    }

    /** Arms a deadline that aborts {@code channel} after {@code timeout}. */
    public Deadline arm(Transport.Channel channel, Duration timeout) {
        return new Deadline(channel, timeout);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }

    /** A running deadline. Close it once the guarded operation has finished. */
    public final class Deadline implements AutoCloseable {
        private final AtomicBoolean expired = new AtomicBoolean();
        private final ScheduledFuture<?> task;

        private Deadline(Transport.Channel channel, Duration timeout) {
            this.task = timer.schedule(() -> {
                expired.set(true);
                channel.abort();
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        public boolean expired() {
            return expired.get();
        }

        /**
         * Converts a failure caused by this deadline into a {@link RiftTimeoutException};
         * other failures are returned unchanged.
         */
        public IOException translate(IOException error, String operation) {
            if (expired() && !(error instanceof RiftTimeoutException)) {
                return new RiftTimeoutException(operation + " timed out", error);
            }
            return error;
        }

        @Override
        public void close() {
            task.cancel(false);
        }
    }
}
