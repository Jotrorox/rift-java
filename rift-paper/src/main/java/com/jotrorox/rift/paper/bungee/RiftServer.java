package com.jotrorox.rift.paper.bungee;

import java.util.Objects;
import java.util.OptionalInt;

/**
 * A running server as listed by Rift's {@code RiftServers} request.
 *
 * @param name the server name as configured in Rift
 * @param group the service group the server is an instance of, or an empty string
 * @param state whether the server accepts new players
 * @param players players currently on the server, as {@code PlayerCount} reports them
 * @param maxPlayers the player limit Rift knows for the server, or zero for none; the
 *        backend's own {@code max-players} still applies
 */
public record RiftServer(String name, String group, State state, int players, int maxPlayers) {
    public RiftServer {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(state, "state");
    }

    /** Whether a server takes new players. */
    public enum State {
        /** The server is up and accepts players. */
        RUNNING,
        /** The server keeps its players but accepts no new ones. */
        DRAINING;

        /**
         * Parses Rift's state name. Unknown states map to {@link #DRAINING}, so a
         * state added by a newer Rift sends no players to the server.
         */
        static State parse(String state) {
            return "running".equals(state) ? RUNNING : DRAINING;
        }
    }

    /** Whether Rift sends new players to this server. */
    public boolean acceptsPlayers() {
        return state == State.RUNNING;
    }

    /** The player limit Rift knows for the server, if any. */
    public OptionalInt limit() {
        return maxPlayers == 0 ? OptionalInt.empty() : OptionalInt.of(maxPlayers);
    }
}
