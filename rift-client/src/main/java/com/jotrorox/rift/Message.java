package com.jotrorox.rift;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * A message delivered by the broker.
 *
 * <p>The payload array is not copied for performance; treat it as read-only.
 *
 * @param subject the literal subject the message was published to
 * @param reply   the reply subject, or {@code null} when the publisher did not set one
 * @param payload the raw payload bytes
 */
public record Message(String subject, String reply, byte[] payload) {
    public Message {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(payload, "payload");
        if (reply != null && reply.isEmpty()) {
            reply = null;
        }
    }

    /** The reply subject, if the publisher expects an answer. */
    public Optional<String> replySubject() {
        return Optional.ofNullable(reply);
    }

    /** The payload decoded as UTF-8. */
    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Message message
                && subject.equals(message.subject)
                && Objects.equals(reply, message.reply)
                && Arrays.equals(payload, message.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, reply, Arrays.hashCode(payload));
    }

    @Override
    public String toString() {
        return "Message[subject=" + subject + ", reply=" + reply + ", payload=" + payload.length + " bytes]";
    }
}
