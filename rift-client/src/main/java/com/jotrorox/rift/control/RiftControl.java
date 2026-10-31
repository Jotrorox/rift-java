package com.jotrorox.rift.control;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftClient;
import com.jotrorox.rift.RiftException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Operates the proxy through Rift's {@code rift.control.*} subjects.
 *
 * <p>The principal needs {@code control = true} in Rift's configuration plus a
 * publish grant for the subjects it uses, for example {@code rift.control.*}.
 * Rift executes control commands one at a time. A command that timed out may
 * still have run: check {@link #status()} before retrying a mutation.
 */
public final class RiftControl {
    /** Default wait for control replies; Rift itself gives up on a command after 40 seconds. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(45);

    private final RiftClient client;
    private final Duration timeout;

    public RiftControl(RiftClient client) {
        this(client, DEFAULT_TIMEOUT);
    }

    private RiftControl(RiftClient client, Duration timeout) {
        this.client = Objects.requireNonNull(client, "client");
        this.timeout = timeout;
    }

    /** A view of this API that waits up to {@code timeout} for each reply. */
    public RiftControl withTimeout(Duration timeout) {
        return new RiftControl(client, timeout);
    }

    /** Connections, players, backends and maintenance state. */
    public ProxyStatus status() throws IOException {
        return ProxyStatus.parse(execute("status", new JsonObject()).getAsJsonObject());
    }

    /**
     * Enables or disables maintenance, which refuses new logins.
     *
     * @return the maintenance state reported by Rift
     */
    public boolean maintenance(boolean enabled) throws IOException {
        JsonObject arguments = new JsonObject();
        arguments.addProperty("enabled", enabled);
        JsonElement data = execute("maintenance", arguments);
        return data.isJsonObject() && data.getAsJsonObject().has("maintenance")
                ? data.getAsJsonObject().get("maintenance").getAsBoolean()
                : enabled;
    }

    /**
     * Enables or disables draining, which keeps new players away from {@code backend}.
     *
     * @return the draining state reported by Rift
     */
    public boolean drain(String backend, boolean enabled) throws IOException {
        JsonObject arguments = new JsonObject();
        arguments.addProperty("backend", requireBackend(backend));
        arguments.addProperty("enabled", enabled);
        JsonElement data = execute("drain", arguments);
        return data.isJsonObject() && data.getAsJsonObject().has("draining")
                ? data.getAsJsonObject().get("draining").getAsBoolean()
                : enabled;
    }

    /**
     * Moves an online player to {@code backend}, applying the same access,
     * health and protocol checks as other transfers.
     *
     * @param connectionId the player's {@link ProxyStatus.Player#connectionId()}
     * @return Rift's description of the completed transfer
     */
    public JsonElement transfer(long connectionId, String backend) throws IOException {
        if (connectionId <= 0) {
            throw new IllegalArgumentException("connectionId must be positive");
        }
        JsonObject arguments = new JsonObject();
        arguments.addProperty("connection_id", connectionId);
        arguments.addProperty("backend", requireBackend(backend));
        return execute("transfer", arguments);
    }

    /**
     * Moves the online player called {@code playerName} to {@code backend}.
     *
     * @throws ControlException if no such player is online
     */
    public JsonElement transfer(String playerName, String backend) throws IOException {
        ProxyStatus.Player player = status().player(playerName)
                .orElseThrow(() -> new ControlException("transfer", "player " + playerName + " is not online"));
        return transfer(player.connectionId(), backend);
    }

    /**
     * Reloads Rift's configuration file. Invalid configurations are refused
     * and the running configuration stays active.
     *
     * @return Rift's description of the applied configuration
     */
    public JsonElement reload() throws IOException {
        return execute("reload", new JsonObject());
    }

    /**
     * Hosts running {@code rift node} for this proxy, with reachability,
     * labels and capacity. Requires Rift with multi-host support.
     */
    public List<NodeStatus> nodes() throws IOException {
        return NodeStatus.parseAll(execute("nodes", new JsonObject()));
    }

    /** {@link #nodes()} on a background thread. */
    public CompletableFuture<List<NodeStatus>> nodesAsync() {
        return executeAsync("nodes", new JsonObject()).thenApply(NodeStatus::parseAll);
    }

    /** {@link #status()} on a background thread. */
    public CompletableFuture<ProxyStatus> statusAsync() {
        return executeAsync("status", new JsonObject())
                .thenApply(data -> ProxyStatus.parse(data.getAsJsonObject()));
    }

    /**
     * Sends {@code arguments} to {@code rift.control.<command>} and returns the
     * {@code data} of a successful reply. Useful for commands added by newer
     * Rift versions.
     *
     * @throws ControlException if Rift refused the command
     */
    public JsonElement execute(String command, JsonObject arguments) throws IOException {
        Message reply = client.request(subject(command), arguments.toString().getBytes(StandardCharsets.UTF_8),
                timeout);
        return unwrap(command, reply.payload());
    }

    /** {@link #execute(String, JsonObject)} on a background thread. */
    public CompletableFuture<JsonElement> executeAsync(String command, JsonObject arguments) {
        return client.requestAsync(subject(command), arguments.toString().getBytes(StandardCharsets.UTF_8), timeout)
                .thenApply(reply -> {
                    try {
                        return unwrap(command, reply.payload());
                    } catch (RiftException e) {
                        throw new java.util.concurrent.CompletionException(e);
                    }
                });
    }

    static JsonElement unwrap(String command, byte[] payload) throws RiftException {
        JsonObject response;
        try {
            response = JsonParser.parseString(new String(payload, StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (JsonParseException | IllegalStateException e) {
            throw new RiftException("invalid control reply for " + command, e);
        }
        JsonElement ok = response.get("ok");
        if (ok != null && ok.isJsonPrimitive() && ok.getAsBoolean()) {
            JsonElement data = response.get("data");
            return data == null ? JsonNull.INSTANCE : data;
        }
        JsonElement error = response.get("error");
        throw new ControlException(command, error != null && error.isJsonPrimitive()
                ? error.getAsString()
                : "command failed");
    }

    private static String subject(String command) {
        if (command == null || command.isEmpty() || command.contains(".") || command.contains("*")
                || command.contains(">")) {
            throw new IllegalArgumentException("invalid control command: " + command);
        }
        return "rift.control." + command;
    }

    private static String requireBackend(String backend) {
        if (backend == null || backend.isEmpty()
                || backend.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw new IllegalArgumentException("backend must be a nonempty name of at most 256 bytes");
        }
        return backend;
    }
}
