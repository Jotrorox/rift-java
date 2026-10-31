package com.jotrorox.rift.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An authenticated connection that opens independent bidirectional streams.
 * The QUIC implementation lives in {@link QuicTransport}; tests substitute an
 * in-memory implementation.
 */
public interface Transport extends AutoCloseable {
    /** Opens a new client-initiated bidirectional stream. */
    Channel open() throws IOException;

    boolean isOpen();

    /** Registers a callback that runs once when the connection terminates for any reason. */
    void onClose(Runnable callback);

    @Override
    void close();

    /** One bidirectional stream. */
    interface Channel {
        InputStream input();

        OutputStream output();

        /** Finishes the sending half; the receiving half stays readable. */
        void finish();

        /** Resets the sending half and stops the receiving half, unblocking pending reads. */
        void abort();
    }
}
