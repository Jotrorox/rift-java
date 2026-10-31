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
