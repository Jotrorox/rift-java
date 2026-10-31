package com.jotrorox.rift.paper.bungee;

/**
 * No player is online to carry a BungeeCord plugin message. Plugin messages
 * travel over player connections, so an empty server cannot send them.
 */
public class NoCarrierException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public NoCarrierException() {
        super("no online player can carry the BungeeCord plugin message");
    }
}
