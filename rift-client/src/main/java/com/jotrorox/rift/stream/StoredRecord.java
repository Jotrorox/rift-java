package com.jotrorox.rift.stream;

import java.time.Instant;

/**
 * Confirmation that a record reached a retained stream's durability boundary.
 *
 * @param sequence        the record's sequence number in its stream
 * @param timestampMillis the time Rift stored the record, in Unix milliseconds
 */
public record StoredRecord(long sequence, long timestampMillis) {
    public Instant timestamp() {
        return Instant.ofEpochMilli(timestampMillis);
    }
}
