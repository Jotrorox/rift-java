package com.jotrorox.rift;

/**
 * Rift rejected an operation, for example because of an invalid subject, an
 * unknown stream, an exceeded limit or a request without responders.
 */
public class RiftRejectedException extends RiftException {
    private static final long serialVersionUID = 1L;

    public RiftRejectedException(String message) {
        super(message);
    }
}
