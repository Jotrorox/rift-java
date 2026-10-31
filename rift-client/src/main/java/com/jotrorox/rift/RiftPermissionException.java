package com.jotrorox.rift;

/**
 * Rift refused the operation because the authenticated principal lacks the
 * required publish or subscribe grant, or because authentication failed.
 */
public class RiftPermissionException extends RiftException {
    private static final long serialVersionUID = 1L;

    public RiftPermissionException(String message) {
        super(message);
    }
}
