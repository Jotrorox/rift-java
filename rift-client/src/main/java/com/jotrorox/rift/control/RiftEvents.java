package com.jotrorox.rift.control;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;

import java.util.Optional;

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
            default -> null;
        });
    }

    private static String text(JsonObject body, String field) {
        JsonElement value = body.get(field);
        return value != null && value.isJsonPrimitive() ? value.getAsString() : "";
    }

    /** An event published by Rift. */
    public sealed interface Event permits Lifecycle, Reload, Control {
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
}
