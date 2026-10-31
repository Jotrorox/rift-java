package com.jotrorox.rift;

import java.io.IOException;

/**
 * Base class for failures reported by Rift or by the messaging transport.
 *
 * <p>A failed publication may already have reached the broker, so retrying can
 * duplicate messages. Applications that need idempotency should carry their own
 * message identifiers.
 */
public class RiftException extends IOException {
    private static final long serialVersionUID = 1L;

    public RiftException(String message) {
        super(message);
    }

    public RiftException(String message, Throwable cause) {
        super(message, cause);
    }
}
