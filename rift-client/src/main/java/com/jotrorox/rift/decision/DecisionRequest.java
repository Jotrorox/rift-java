package com.jotrorox.rift.decision;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;

import java.util.UUID;

/**
 * A login or transfer Rift asks a subscriber to decide. Fields a hook does not
 * have are {@code null}.
 *
 * @param hook          {@code login}, {@code initial_server} or {@code before_transfer}
 * @param uuid          the player's Mojang-verified UUID
 * @param name          the player's Mojang name
 * @param connectionId  Rift's connection id
 * @param peerIp        the client's IP address
 * @param hostname      the normalized hostname the client connected to
 * @param protocol      the client's protocol version
 * @param defaultServer the server routing proposes for this login, or {@code null}
 * @param server        the server the player is on, or {@code null} before the first join
 * @param target        the transfer target, for {@code before_transfer}
 * @param reason        why the transfer happens, such as {@code command} or {@code recovery}
 */
public record DecisionRequest(
        String hook,
        UUID uuid,
        String name,
        long connectionId,
        String peerIp,
        String hostname,
        int protocol,
        String defaultServer,
        String server,
        String target,
        String reason) {

    public static final String LOGIN = "login";
    public static final String INITIAL_SERVER = "initial_server";
    public static final String BEFORE_TRANSFER = "before_transfer";

    /**
     * Parses the request Rift sends on a decision subject.
     *
     * @throws IllegalArgumentException if the payload is not a decision request
     */
    public static DecisionRequest parse(Message message) {
        JsonObject body;
        try {
            JsonElement element = JsonParser.parseString(message.payloadAsString());
            if (!element.isJsonObject()) {
                throw new IllegalArgumentException("decision request must be a JSON object");
            }
            body = element.getAsJsonObject();
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("decision request must be a JSON object", e);
        }
        try {
            return new DecisionRequest(
                    required(body, "hook"),
                    uuid(required(body, "uuid")),
                    required(body, "name"),
                    Long.parseUnsignedLong(required(body, "connection_id")),
                    required(body, "peer_ip"),
                    required(body, "hostname"),
                    body.get("protocol").getAsInt(),
                    optional(body, "default_server"),
                    optional(body, "server"),
                    optional(body, "target"),
                    optional(body, "reason"));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("malformed decision request: " + e.getMessage(), e);
        }
    }

    /** Rift sends UUIDs as 32 lowercase hex digits without hyphens. */
    static UUID uuid(String hex) {
        if (hex.length() != 32) {
            throw new IllegalArgumentException("uuid must have 32 hex digits");
        }
        return new UUID(Long.parseUnsignedLong(hex.substring(0, 16), 16),
                Long.parseUnsignedLong(hex.substring(16), 16));
    }

    private static String required(JsonObject body, String field) {
        String value = optional(body, field);
        if (value == null) {
            throw new IllegalArgumentException("missing " + field);
        }
        return value;
    }

    private static String optional(JsonObject body, String field) {
        JsonElement value = body.get(field);
        return value == null || value.isJsonNull() ? null : value.getAsString();
    }
}
