package com.jotrorox.rift.stream;

import java.io.IOException;
import java.util.List;

/**
 * A durable consumer of a retained stream, owned by the authenticated principal.
 *
 * <p>Delivery is at least once: acknowledge each record only after it was
 * processed. Several workers of the same principal can share a consumer name
 * to divide its records.
 */
public interface StreamConsumer {
    /** The stream this consumer reads. */
    String stream();

    /** The consumer name, scoped to the authenticated principal. */
    String name();

    /**
     * Fetches up to {@code limit} currently available records without waiting
     * for new ones.
     *
     * @param limit 1 to 256
     */
    List<Delivery> fetch(int limit) throws IOException;

    /** Acknowledges a processed record. */
    void ack(long sequence) throws IOException;

    /** Acknowledges a processed delivery. */
    default void ack(Delivery delivery) throws IOException {
        ack(delivery.sequence());
    }
}
