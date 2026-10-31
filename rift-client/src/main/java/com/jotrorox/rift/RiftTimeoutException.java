package com.jotrorox.rift;

/**
 * An operation did not complete before its deadline. The operation may still
 * have taken effect on the server: check state before retrying a mutation.
 */
public class RiftTimeoutException extends RiftException {
    private static final long serialVersionUID = 1L;

    public RiftTimeoutException(String message) {
        super(message);
    }

    public RiftTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
