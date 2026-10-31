package com.jotrorox.rift.decision;

import com.google.gson.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The answer to a {@link DecisionRequest}. Rift validates it exactly like a
 * Lua hook result, so an invalid answer rejects the login or skips the transfer.
 */
public sealed interface Decision permits Decision.Proceed, Decision.Deny, Decision.Server {
    /** Let the login or transfer go ahead. */
    static Decision proceed() {
        return Proceed.INSTANCE;
    }

    /** Refuse it; Rift shows {@code reason} to the player. */
    static Decision deny(String reason) {
        return new Deny(reason);
    }

    /** Send the player to {@code server}. Rift accepts this only for {@code initial_server}. */
    static Decision server(String server) {
        return new Server(server);
    }

    /** The JSON reply Rift expects. */
    String toJson();

    /** Continue with the login or transfer. */
    final class Proceed implements Decision {
        private static final Proceed INSTANCE = new Proceed();

        private Proceed() {
        }

        @Override
        public String toJson() {
            return "{\"action\":\"continue\"}";
        }

        @Override
        public String toString() {
            return "Decision.proceed()";
        }
    }

    /** @param reason shown to the player, 1 to 1024 UTF-8 bytes */
    record Deny(String reason) implements Decision {
        public Deny {
            requireText(reason, "reason");
        }

        @Override
        public String toJson() {
            JsonObject json = new JsonObject();
            json.addProperty("action", "deny");
            json.addProperty("reason", reason);
            return json.toString();
        }
    }

    /** @param server a server or service group configured in Rift */
    record Server(String server) implements Decision {
        public Server {
            requireText(server, "server");
        }

        @Override
        public String toJson() {
            JsonObject json = new JsonObject();
            json.addProperty("action", "server");
            json.addProperty("server", server);
            return json.toString();
        }
    }

    private static void requireText(String text, String field) {
        Objects.requireNonNull(text, field);
        int length = text.getBytes(StandardCharsets.UTF_8).length;
        if (length < 1 || length > 1024) {
            throw new IllegalArgumentException(field + " must contain 1 to 1024 UTF-8 bytes");
        }
    }
}
