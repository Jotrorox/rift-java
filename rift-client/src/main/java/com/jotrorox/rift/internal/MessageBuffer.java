package com.jotrorox.rift.internal;

import com.jotrorox.rift.Message;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded single-producer buffer that can be ended. Consumers drain the
 * remaining messages after the end before they observe it.
 */
public final class MessageBuffer {
    private final int capacity;
    private final ArrayDeque<Message> messages = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private boolean ended;

    public MessageBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
    }

    /**
     * Adds a message, blocking while the buffer is full.
     *
     * @return {@code false} if the buffer has ended and the message was dropped
     */
    public boolean put(Message message) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (messages.size() >= capacity && !ended) {
                notFull.await();
            }
            if (ended) {
                return false;
            }
            messages.add(message);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the next message, waiting up to {@code timeout} nanoseconds
     * ({@code Long.MAX_VALUE} waits indefinitely).
     *
     * @return the message, or {@code null} if the wait timed out or the buffer
     *         ended and is empty; check {@link #isDrained()} to tell them apart
     */
    public Message take(long timeout, TimeUnit unit) throws InterruptedException {
        long remaining = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (messages.isEmpty() && !ended) {
                if (timeout == Long.MAX_VALUE) {
                    notEmpty.await();
                } else {
                    if (remaining <= 0) {
                        return null;
                    }
                    remaining = notEmpty.awaitNanos(remaining);
                }
            }
            Message message = messages.poll();
            if (message != null) {
                notFull.signal();
            }
            return message;
        } finally {
            lock.unlock();
        }
    }

    /** Ends the buffer; with {@code discard}, buffered messages are dropped as well. */
    public void end(boolean discard) {
        lock.lock();
        try {
            ended = true;
            if (discard) {
                messages.clear();
            }
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Whether the buffer has ended and every message was taken. */
    public boolean isDrained() {
        lock.lock();
        try {
            return ended && messages.isEmpty();
        } finally {
            lock.unlock();
        }
    }
}
