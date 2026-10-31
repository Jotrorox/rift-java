package com.jotrorox.rift;

/**
 * Receives messages from a subscription.
 *
 * <p>Handlers run on the subscription's own delivery thread, one message at a
 * time and in delivery order. A slow handler slows down only its own
 * subscription; if it falls far enough behind, Rift disconnects that
 * subscription as a slow consumer. Exceptions are logged and do not end the
 * subscription.
 */
@FunctionalInterface
public interface MessageHandler {
    void onMessage(Message message) throws Exception;
}
