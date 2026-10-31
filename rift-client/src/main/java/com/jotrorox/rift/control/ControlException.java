package com.jotrorox.rift.control;

import com.jotrorox.rift.RiftException;

/** Rift received a control command and refused it, for example an unknown backend. */
public class ControlException extends RiftException {
    private static final long serialVersionUID = 1L;

    private final String command;

    public ControlException(String command, String message) {
        super(command + ": " + message);
        this.command = command;
    }

    /** The control command, such as {@code drain}. */
    public String command() {
        return command;
    }
}
