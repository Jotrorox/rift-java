package com.jotrorox.rift.decision;

import com.google.gson.JsonParser;
import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftClient;
import com.jotrorox.rift.RiftException;
import com.jotrorox.rift.Subscription;
import com.jotrorox.rift.testing.FakeRift;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DecisionTest {
    private static final Duration WAIT = Duration.ofSeconds(2);
    private static final String LOGIN = """
            {"hook":"login","uuid":"069a79f444e94726a5befca90e38aaf5","name":"Notch",
             "connection_id":"7","peer_ip":"203.0.113.4","hostname":"play.example","protocol":772,
             "default_server":"lobby","server":null,"target":null,"reason":null}""";

    private static Message message(String json) {
        return new Message("relaxoban.login", "_INBOX.x", json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesRequests() {
        DecisionRequest request = DecisionRequest.parse(message(LOGIN));
        assertEquals(new DecisionRequest("login", UUID.fromString("069a79f4-44e9-4726-a5be-fca90e38aaf5"),
                "Notch", 7, "203.0.113.4", "play.example", 772, "lobby", null, null, null), request);
        for (String invalid : new String[]{"", "[]", "{}", LOGIN.replace("069a79f4", "xyz"),
                LOGIN.replace("\"name\":\"Notch\",", "")}) {
            assertThrows(IllegalArgumentException.class, () -> DecisionRequest.parse(message(invalid)), invalid);
        }
    }

    @Test
    void serializesDecisions() {
        assertEquals(JsonParser.parseString("{\"action\":\"continue\"}"),
                JsonParser.parseString(Decision.proceed().toJson()));
        assertEquals(JsonParser.parseString("{\"action\":\"deny\",\"reason\":\"Banned \\\"for\\\" griefing\"}"),
                JsonParser.parseString(Decision.deny("Banned \"for\" griefing").toJson()));
        assertEquals(JsonParser.parseString("{\"action\":\"server\",\"server\":\"lobby-2\"}"),
                JsonParser.parseString(Decision.server("lobby-2").toJson()));
        assertThrows(IllegalArgumentException.class, () -> Decision.deny(""));
        assertThrows(IllegalArgumentException.class, () -> Decision.deny("ä".repeat(513)));
        assertThrows(NullPointerException.class, () -> Decision.server(null));
    }

    @Test
    void answersRequestsWithTheHandlersDecision() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient decider = rift.connect(); RiftClient proxy = rift.connect()) {
            Subscription subscription = decider.decide("relaxoban.login", request ->
                    CompletableFuture.supplyAsync(() -> request.name().equals("Notch")
                            ? Decision.deny("You are banned")
                            : Decision.proceed()));
            assertEquals(RiftClient.DEFAULT_DECIDER_GROUP, subscription.queueGroup());
            Message reply = proxy.request("relaxoban.login", LOGIN, WAIT);
            assertEquals("{\"action\":\"deny\",\"reason\":\"You are banned\"}", reply.payloadAsString());
            reply = proxy.request("relaxoban.login", LOGIN.replace("Notch", "Alex"), WAIT);
            assertEquals("{\"action\":\"continue\"}", reply.payloadAsString());
        }
    }

    @Test
    void failedDecisionsSendNoReply() throws Exception {
        FakeRift rift = new FakeRift();
        try (RiftClient decider = rift.connect(); RiftClient proxy = rift.connect()) {
            decider.decide("relaxoban.login", request ->
                    CompletableFuture.failedFuture(new IllegalStateException("database down")));
            decider.decide("relaxoban.transfer", null, request -> {
                throw new IllegalStateException("broken");
            });
            assertThrows(RiftException.class,
                    () -> proxy.request("relaxoban.login", LOGIN, Duration.ofMillis(200)));
            assertThrows(RiftException.class,
                    () -> proxy.request("relaxoban.transfer", LOGIN, Duration.ofMillis(200)));
            // A malformed request is logged and not answered.
            assertThrows(RiftException.class,
                    () -> proxy.request("relaxoban.login", "{}", Duration.ofMillis(200)));
        }
    }

    @Test
    void rejectsWildcardSubjects() throws Exception {
        try (RiftClient client = new FakeRift().connect()) {
            assertThrows(IllegalArgumentException.class,
                    () -> client.decide("relaxoban.*", request -> null));
        }
    }

    @Test
    void parsesPlayerEvents() {
        Message message = new Message("rift.events.players", null, """
                {"event":"switch","connection_id":"9","uuid":"069a79f444e94726a5befca90e38aaf5",
                 "name":"Notch","server":"game","previous":"lobby"}""".getBytes(StandardCharsets.UTF_8));
        var event = (com.jotrorox.rift.control.RiftEvents.Player)
                com.jotrorox.rift.control.RiftEvents.parse(message).orElseThrow();
        assertEquals(UUID.fromString("069a79f4-44e9-4726-a5be-fca90e38aaf5"), event.uuid());
        assertEquals("game", event.server());
        assertEquals("lobby", event.previous());
        assertEquals(9, event.connectionId());
        Message quit = new Message("rift.events.players", null, """
                {"event":"quit","connection_id":"9","uuid":"069a79f444e94726a5befca90e38aaf5",
                 "name":"Notch","server":"game"}""".getBytes(StandardCharsets.UTF_8));
        var left = (com.jotrorox.rift.control.RiftEvents.Player)
                com.jotrorox.rift.control.RiftEvents.parse(quit).orElseThrow();
        assertNull(left.previous());
        assertEquals(true, left.isQuit());
    }
}
