package com.jotrorox.rift.control;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlParsingTest {
    private static byte[] bytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void unwrapsSuccessfulReplies() throws RiftException {
        JsonElement data = RiftControl.unwrap("maintenance", bytes("{\"ok\":true,\"data\":{\"maintenance\":true}}"));
        assertTrue(data.getAsJsonObject().get("maintenance").getAsBoolean());
        assertTrue(RiftControl.unwrap("reload", bytes("{\"ok\":true}")).isJsonNull());
    }

    @Test
    void reportsRefusedCommands() {
        ControlException error = assertThrows(ControlException.class,
                () -> RiftControl.unwrap("drain", bytes("{\"ok\":false,\"error\":\"unknown backend\"}")));
        assertEquals("drain", error.command());
        assertEquals("drain: unknown backend", error.getMessage());
        assertThrows(RiftException.class, () -> RiftControl.unwrap("status", bytes("not json")));
        assertThrows(RiftException.class, () -> RiftControl.unwrap("status", bytes("[1]")));
    }

    @Test
    void parsesStatus() {
        String json = """
                {"maintenance":false,"connections":3,"players_online":2,
                 "players":[{"connection_id":7,"name":"Steve","backend":"lobby","protocol":774},
                            {"connection_id":9,"name":"Alex","backend":"survival","protocol":772}],
                 "backends":[{"backend":"lobby","up":true,"draining":false},
                             {"backend":"survival","up":false,"draining":true}],
                 "managed_servers":[{"name":"survival","state":"running"}],
                 "future_field":1}
                """;
        ProxyStatus status = ProxyStatus.parse(JsonParser.parseString(json).getAsJsonObject());
        assertFalse(status.maintenance());
        assertEquals(3, status.connections());
        assertEquals(2, status.playersOnline());
        assertEquals(new ProxyStatus.Player(7, "Steve", "lobby", 774), status.player("steve").orElseThrow());
        assertEquals(1, status.playersOn("survival").size());
        assertEquals(new ProxyStatus.Backend("survival", false, true), status.backend("survival").orElseThrow());
        assertEquals("running", status.managedServers().get(0).getAsJsonObject().get("state").getAsString());
        assertEquals(1, status.raw().get("future_field").getAsInt());
    }

    @Test
    void parsesNodes() {
        String json = """
                {"nodes":[{"name":"node1","state":"online","address":"10.0.0.11","labels":["games"],
                           "memory_mb":32768,"allocated_memory_mb":4096,"instances":2,"max_instances":40,
                           "running":2,"version":"0.1.0","last_seen_ms":800,"connected_ms":60000},
                          {"name":"node2","state":"never_connected","address":"10.0.0.12","labels":[],
                           "memory_mb":null,"allocated_memory_mb":0,"instances":0,"max_instances":128,
                           "running":null,"version":null,"last_seen_ms":null},
                          {"name":"node3","state":"draining"}]}
                """;
        var nodes = NodeStatus.parseAll(JsonParser.parseString(json));
        assertEquals(3, nodes.size());
        NodeStatus first = nodes.get(0);
        assertTrue(first.online());
        assertTrue(first.hasLabel("games"));
        assertEquals(32768, first.memoryMb().orElseThrow());
        assertEquals(4096, first.allocatedMemoryMb());
        assertEquals(2, first.instances());
        assertEquals(40, first.maxInstances());
        assertEquals("0.1.0", first.version().orElseThrow());
        assertEquals(java.time.Duration.ofMillis(800), first.lastSeen().orElseThrow());
        assertEquals(60000, first.raw().get("connected_ms").getAsLong());
        NodeStatus second = nodes.get(1);
        assertEquals(NodeStatus.State.NEVER_CONNECTED, second.state());
        assertFalse(second.online());
        assertTrue(second.memoryMb().isEmpty());
        assertTrue(second.version().isEmpty());
        assertTrue(second.lastSeen().isEmpty());
        assertEquals(NodeStatus.State.OTHER, nodes.get(2).state());
        assertTrue(NodeStatus.parseAll(JsonParser.parseString("{}")).isEmpty());
    }

    @Test
    void parsesSparseStatus() {
        ProxyStatus status = ProxyStatus.parse(JsonParser.parseString("{}").getAsJsonObject());
        assertTrue(status.players().isEmpty());
        assertTrue(status.backends().isEmpty());
        assertEquals(0, status.managedServers().size());
    }

    @Test
    void parsesEvents() {
        RiftEvents.Lifecycle lifecycle = assertInstanceOf(RiftEvents.Lifecycle.class,
                RiftEvents.parse(new Message(RiftEvents.LIFECYCLE, null, bytes("{\"state\":\"draining\"}")))
                        .orElseThrow());
        assertTrue(lifecycle.isDraining());
        assertFalse(lifecycle.isReady());
        assertEquals(new RiftEvents.Reload("abc123"),
                RiftEvents.parse(new Message(RiftEvents.RELOAD, null, bytes("{\"revision\":\"abc123\"}")))
                        .orElseThrow());
        assertEquals(new RiftEvents.Control("drain", false),
                RiftEvents.parse(new Message(RiftEvents.CONTROL, null, bytes("{\"command\":\"drain\",\"ok\":false}")))
                        .orElseThrow());
        assertTrue(RiftEvents.parse(new Message("rift.events.other", null, bytes("{}"))).isEmpty());
        assertTrue(RiftEvents.parse(new Message(RiftEvents.RELOAD, null, bytes("{"))).isEmpty());
    }
}
