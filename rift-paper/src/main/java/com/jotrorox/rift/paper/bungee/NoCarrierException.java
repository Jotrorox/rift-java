package com.jotrorox.rift.paper.bungee;

/**
 * No online player can carry a BungeeCord plugin message. Plugin messages
 * travel over player connections, and only once the proxy has registered the
 * channel on them, so an empty server, or one whose players are all still
 * joining, cannot send them.
 */
public class NoCarrierException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public NoCarrierException() {
        super("no online player can carry the BungeeCord plugin message");
    }
}
