package com.jotrorox.rift.control;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A host running {@code rift node}, as returned by {@code rift.control.nodes}.
 *
 * <p>The proxy is the controller: it places game servers on nodes and the
 * agents run them. Servers on an {@link State#UNREACHABLE} node report the
 * state {@code unknown}.
 *
 * @param name              the node name from {@code rift.config.nodes}
 * @param state             whether the agent currently reports to the proxy
 * @param address           the address the proxy connects to for game traffic
 * @param labels            labels that service groups select nodes by
 * @param memoryMb          memory available for instances, empty without a limit
 * @param allocatedMemoryMb memory allocated to the node's instances
 * @param instances         instances and named servers placed on the node
 * @param maxInstances      the most instances the node may run
 * @param version           the agent's Rift version, once it connected
 * @param lastSeen          time since the agent's latest state report, once it connected
 * @param raw               the complete node object, including fields added by newer Rift versions
 */
public record NodeStatus(String name, State state, String address, List<String> labels, OptionalLong memoryMb,
                         long allocatedMemoryMb, long instances, long maxInstances, Optional<String> version,
                         Optional<Duration> lastSeen, JsonObject raw) {
    public NodeStatus {
        labels = List.copyOf(labels);
    }

    /** Reachability of a node agent. */
    public enum State {
        /** The agent reported its state within the last 10 seconds. */
        ONLINE,
        /** The agent connected before but has stopped reporting. */
        UNREACHABLE,
        /** The agent has not connected since the proxy started. */
        NEVER_CONNECTED,
        /** A state introduced by a newer Rift version. */
        OTHER;

        static State parse(String value) {
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return OTHER;
            }
        }
    }

    /** Whether new servers can be placed on this node right now. */
    public boolean online() {
        return state == State.ONLINE;
    }

    /** Whether the node has the label {@code label}. */
    public boolean hasLabel(String label) {
        return labels.contains(label);
    }

    static List<NodeStatus> parseAll(JsonElement data) {
        List<NodeStatus> nodes = new ArrayList<>();
        if (data == null || !data.isJsonObject()) {
            return nodes;
        }
        JsonElement values = data.getAsJsonObject().get("nodes");
        if (values == null || !values.isJsonArray()) {
            return nodes;
        }
        for (JsonElement element : values.getAsJsonArray()) {
            if (element.isJsonObject()) {
                nodes.add(parse(element.getAsJsonObject()));
            }
        }
        return nodes;
    }

    static NodeStatus parse(JsonObject node) {
        List<String> labels = new ArrayList<>();
        JsonElement labelValues = node.get("labels");
        if (labelValues != null && labelValues.isJsonArray()) {
            for (JsonElement label : (JsonArray) labelValues) {
                labels.add(label.getAsString());
            }
        }
        return new NodeStatus(
                string(node, "name"),
                State.parse(string(node, "state")),
                string(node, "address"),
                labels,
                present(node, "memory_mb") ? OptionalLong.of(node.get("memory_mb").getAsLong()) : OptionalLong.empty(),
                number(node, "allocated_memory_mb"),
                number(node, "instances"),
                number(node, "max_instances"),
                present(node, "version") ? Optional.of(node.get("version").getAsString()) : Optional.empty(),
                present(node, "last_seen_ms")
                        ? Optional.of(Duration.ofMillis(node.get("last_seen_ms").getAsLong()))
                        : Optional.empty(),
                node);
    }

    private static boolean present(JsonObject object, String field) {
        JsonElement value = object.get(field);
        return value != null && !value.isJsonNull();
    }

    private static String string(JsonObject object, String field) {
        return present(object, field) ? object.get(field).getAsString() : "";
    }

    private static long number(JsonObject object, String field) {
        return present(object, field) ? object.get(field).getAsLong() : 0;
    }
}
