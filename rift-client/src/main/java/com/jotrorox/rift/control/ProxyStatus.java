package com.jotrorox.rift.control;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A snapshot of the proxy returned by {@code rift.control.status}.
 *
 * @param maintenance    whether new logins are refused for maintenance
 * @param connections    open client connections, including those still logging in
 * @param playersOnline  players that completed login
 * @param players        online players with their current backend
 * @param backends       configured backends with health and draining state
 * @param managedServers Minecraft processes supervised by Rift, as reported by the proxy
 * @param raw            the complete status object, including fields added by newer Rift versions
 */
public record ProxyStatus(boolean maintenance, long connections, long playersOnline, List<Player> players,
                          List<Backend> backends, JsonArray managedServers, JsonObject raw) {
    public ProxyStatus {
        players = List.copyOf(players);
        backends = List.copyOf(backends);
    }

    /**
     * An online player.
     *
     * @param connectionId the connection identifier used by {@link RiftControl#transfer}
     * @param name         the player name
     * @param backend      the backend the player is connected to
     * @param protocol     the client's Minecraft protocol version number
     */
    public record Player(long connectionId, String name, String backend, int protocol) {
    }

    /**
     * A configured backend.
     *
     * @param name     the backend name
     * @param up       whether the latest health check succeeded
     * @param draining whether new players are kept away from the backend
     */
    public record Backend(String name, boolean up, boolean draining) {
    }

    /** Finds an online player by name, ignoring ASCII case. */
    public Optional<Player> player(String name) {
        return players.stream().filter(player -> player.name().equalsIgnoreCase(name)).findFirst();
    }

    /** Finds a backend by name. */
    public Optional<Backend> backend(String name) {
        return backends.stream().filter(backend -> backend.name().equals(name)).findFirst();
    }

    /** Players currently connected to {@code backend}. */
    public List<Player> playersOn(String backend) {
        return players.stream().filter(player -> player.backend().equals(backend)).toList();
    }

    static ProxyStatus parse(JsonObject data) {
        List<Player> players = new ArrayList<>();
        for (JsonElement element : array(data, "players")) {
            JsonObject player = element.getAsJsonObject();
            players.add(new Player(
                    player.get("connection_id").getAsLong(),
                    string(player, "name"),
                    string(player, "backend"),
                    player.has("protocol") ? player.get("protocol").getAsInt() : 0));
        }
        List<Backend> backends = new ArrayList<>();
        for (JsonElement element : array(data, "backends")) {
            JsonObject backend = element.getAsJsonObject();
            backends.add(new Backend(string(backend, "backend"), bool(backend, "up"), bool(backend, "draining")));
        }
        return new ProxyStatus(
                bool(data, "maintenance"),
                number(data, "connections"),
                number(data, "players_online"),
                players,
                backends,
                array(data, "managed_servers"),
                data);
    }

    private static JsonArray array(JsonObject object, String field) {
        JsonElement value = object.get(field);
        return value != null && value.isJsonArray() ? value.getAsJsonArray() : new JsonArray();
    }

    private static String string(JsonObject object, String field) {
        JsonElement value = object.get(field);
        return value != null && !value.isJsonNull() ? value.getAsString() : "";
    }

    private static boolean bool(JsonObject object, String field) {
        JsonElement value = object.get(field);
        return value != null && !value.isJsonNull() && value.getAsBoolean();
    }

    private static long number(JsonObject object, String field) {
        JsonElement value = object.get(field);
        return value != null && !value.isJsonNull() ? value.getAsLong() : 0;
    }
}
