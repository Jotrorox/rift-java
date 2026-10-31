package com.jotrorox.rift.control;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;

import java.util.Optional;
import java.util.UUID;

/**
 * Events Rift publishes on {@code rift.events.*}. They are ephemeral: query
 * {@link RiftControl#status()} for the current state after reconnecting.
 * Subscribing requires a matching subscribe grant, such as {@code rift.events.>}.
 */
public final class RiftEvents {
    /** Every Rift event. */
    public static final String ALL = "rift.events.>";
    /** Proxy lifecycle: {@code ready} after startup, {@code draining} on shutdown. */
    public static final String LIFECYCLE = "rift.events.lifecycle";
    /** A configuration reload succeeded. */
    public static final String RELOAD = "rift.events.reload";
    /** A control command other than status finished. */
    public static final String CONTROL = "rift.events.control";
    /** A player joined, switched servers or quit. */
    public static final String PLAYERS = "rift.events.players";

    private RiftEvents() {
    }

    /**
     * Parses any Rift event message.
     *
     * @return the event, or empty for unknown subjects or malformed payloads
     */
    public static Optional<Event> parse(Message message) {
        JsonObject body;
        try {
            JsonElement element = JsonParser.parseString(message.payloadAsString());
            if (!element.isJsonObject()) {
                return Optional.empty();
            }
            body = element.getAsJsonObject();
        } catch (JsonParseException e) {
            return Optional.empty();
        }
        return Optional.ofNullable(switch (message.subject()) {
            case LIFECYCLE -> new Lifecycle(text(body, "state"));
            case RELOAD -> new Reload(text(body, "revision"));
            case CONTROL -> new Control(text(body, "command"),
                    body.has("ok") && body.get("ok").isJsonPrimitive() && body.get("ok").getAsBoolean());
            case PLAYERS -> player(body);
            default -> null;
        });
    }

    private static Player player(JsonObject body) {
        String hex = text(body, "uuid");
        if (hex.length() != 32) {
            return null;
        }
        try {
            UUID uuid = new UUID(Long.parseUnsignedLong(hex.substring(0, 16), 16),
                    Long.parseUnsignedLong(hex.substring(16), 16));
            String previous = text(body, "previous");
            return new Player(text(body, "event"), uuid, text(body, "name"), text(body, "server"),
                    previous.isEmpty() ? null : previous, Long.parseUnsignedLong(text(body, "connection_id")));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String text(JsonObject body, String field) {
        JsonElement value = body.get(field);
        return value != null && value.isJsonPrimitive() ? value.getAsString() : "";
    }

    /** An event published by Rift. */
    public sealed interface Event permits Lifecycle, Reload, Control, Player {
    }

    /**
     * The proxy changed lifecycle state.
     *
     * @param state {@code ready} or {@code draining}
     */
    public record Lifecycle(String state) implements Event {
        public boolean isReady() {
            return state.equals("ready");
        }

        public boolean isDraining() {
            return state.equals("draining");
        }
    }

    /**
     * A configuration reload was applied.
     *
     * @param revision the active configuration revision
     */
    public record Reload(String revision) implements Event {
    }

    /**
     * A control command finished.
     *
     * @param command the command name, such as {@code drain}
     * @param ok      whether it succeeded
     */
    public record Control(String command, boolean ok) implements Event {
    }

    /**
     * A player joined the network, switched servers or quit.
     *
     * @param event        {@code join}, {@code switch} or {@code quit}
     * @param uuid         the player's UUID
     * @param name         the player's name
     * @param server       the new server, or for {@code quit} the last one
     * @param previous     the server a {@code switch} came from, otherwise {@code null}
     * @param connectionId Rift's connection id
     */
    public record Player(String event, UUID uuid, String name, String server, String previous, long connectionId)
            implements Event {
        public boolean isJoin() {
            return event.equals("join");
        }

        public boolean isSwitch() {
            return event.equals("switch");
        }

        public boolean isQuit() {
            return event.equals("quit");
        }
    }
}
