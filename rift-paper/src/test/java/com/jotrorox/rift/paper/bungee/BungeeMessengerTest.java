package com.jotrorox.rift.paper.bungee;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Wire formats follow Rift's BungeeCord compatibility documentation. */
class BungeeMessengerTest {
    private final List<Sent> sent = new ArrayList<>();
    private String online = "Steve";
    private BungeeMessenger<String> messenger;

    private record Sent(String carrier, byte[] message) {
    }

    @BeforeEach
    void setUp() {
        messenger = new BungeeMessenger<>(new BungeeMessenger.Carriers<>() {
            @Override
            public Optional<String> any() {
                return Optional.ofNullable(online);
            }

            @Override
            public Object id(String player) {
                return player;
            }

            @Override
            public void send(String player, byte[] message) {
                sent.add(new Sent(player, message));
            }
        });
        messenger.timeout(Duration.ofMillis(200));
    }

    private static byte[] utf(Object... fields) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            for (Object field : fields) {
                switch (field) {
                    case String text -> out.writeUTF(text);
                    case Integer number -> out.writeInt(number);
                    case Short number -> out.writeShort(number);
                    case byte[] data -> out.write(data);
                    default -> throw new IllegalArgumentException(field.toString());
                }
            }
        }
        return bytes.toByteArray();
    }

    private List<String> lastStrings(int count) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(sent.getLast().message()));
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            strings.add(in.readUTF());
        }
        assertEquals(0, in.available());
        return strings;
    }

    @Test
    void sendsTransfers() throws IOException {
        messenger.connect("Alex", "survival");
        assertEquals("Alex", sent.getLast().carrier());
        assertEquals(List.of("Connect", "survival"), lastStrings(2));
        messenger.connectOther("Alex", "lobby");
        assertEquals("Steve", sent.getLast().carrier());
        assertEquals(List.of("ConnectOther", "Alex", "lobby"), lastStrings(3));
    }

    @Test
    void answersPlayerCountAndListQueries() throws Exception {
        CompletableFuture<Integer> all = messenger.playerCount(BungeeMessenger.ALL);
        CompletableFuture<Integer> survival = messenger.playerCount("Survival");
        CompletableFuture<List<String>> list = messenger.playerList("lobby");
        CompletableFuture<List<String>> empty = messenger.playerList("void");
        assertEquals(List.of("PlayerList", "void"), lastStrings(2));

        // Replies use the configured server name, which may differ in case from the request.
        messenger.receive("Steve", utf("PlayerCount", "survival", 3));
        messenger.receive("Steve", utf("PlayerCount", "ALL", 12));
        messenger.receive("Steve", utf("PlayerList", "lobby", "Alex, Steve"));
        messenger.receive("Steve", utf("PlayerList", "void", ""));

        assertEquals(12, all.get(1, TimeUnit.SECONDS));
        assertEquals(3, survival.get(1, TimeUnit.SECONDS));
        assertEquals(List.of("Alex", "Steve"), list.get(1, TimeUnit.SECONDS));
        assertEquals(List.of(), empty.get(1, TimeUnit.SECONDS));
    }

    @Test
    void answersServerQueries() throws Exception {
        CompletableFuture<List<String>> servers = messenger.servers();
        CompletableFuture<String> current = messenger.serverName();
        CompletableFuture<String> where = messenger.playerServer("alex");
        CompletableFuture<InetSocketAddress> address = messenger.serverAddress("survival");

        messenger.receive("Steve", utf("GetServers", "lobby, survival"));
        messenger.receive("Steve", utf("GetServer", "lobby"));
        messenger.receive("Steve", utf("GetPlayerServer", "Alex", "survival"));
        messenger.receive("Steve", utf("ServerIP", "survival", "10.0.0.5", (short) 0xFFFE));

        assertEquals(List.of("lobby", "survival"), servers.get(1, TimeUnit.SECONDS));
        assertEquals("lobby", current.get(1, TimeUnit.SECONDS));
        assertEquals("survival", where.get(1, TimeUnit.SECONDS));
        InetSocketAddress server = address.get(1, TimeUnit.SECONDS);
        assertEquals("10.0.0.5", server.getHostString());
        assertEquals(65534, server.getPort());
    }

    @Test
    void answersPlayerQueries() throws Exception {
        CompletableFuture<UUID> own = messenger.uuid("Steve" /* carrier */);
        assertEquals(List.of("UUID"), lastStrings(1));
        CompletableFuture<UUID> other = messenger.playerUuid("all");
        CompletableFuture<InetSocketAddress> ip = messenger.playerAddress("Alex");

        messenger.receive("Steve", utf("UUID", "0123456789abcdef0123456789abcdef"));
        messenger.receive("Steve", utf("UUIDOther", "All", "ffffffffffffffff0000000000000001"));
        messenger.receive("Steve", utf("IPOther", "Alex", "203.0.113.7", 51234));

        assertEquals(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"), own.get(1, TimeUnit.SECONDS));
        assertEquals(UUID.fromString("ffffffff-ffff-ffff-0000-000000000001"), other.get(1, TimeUnit.SECONDS));
        assertEquals(InetSocketAddress.createUnresolved("203.0.113.7", 51234), ip.get(1, TimeUnit.SECONDS));
    }

    @Test
    void matchesCarrierSpecificRepliesByCarrier() throws Exception {
        CompletableFuture<String> steve = messenger.serverName("Steve");
        CompletableFuture<String> alex = messenger.serverName("Alex");
        messenger.receive("Alex", utf("GetServer", "survival"));
        assertFalse(steve.isDone());
        assertEquals("survival", alex.get(1, TimeUnit.SECONDS));
        messenger.receive("Steve", utf("GetServer", "lobby"));
        assertEquals("lobby", steve.get(1, TimeUnit.SECONDS));
    }

    @Test
    void repeatedQueriesCompleteInOrder() throws Exception {
        CompletableFuture<Integer> first = messenger.playerCount("lobby");
        CompletableFuture<Integer> second = messenger.playerCount("lobby");
        messenger.receive("Steve", utf("PlayerCount", "lobby", 1));
        messenger.receive("Steve", utf("PlayerCount", "lobby", 2));
        assertEquals(1, first.get(1, TimeUnit.SECONDS));
        assertEquals(2, second.get(1, TimeUnit.SECONDS));
    }

    /** A {@code RiftServers} reply; each entry is name, group, state, players and limit. */
    private static byte[] riftServers(String target, Object[]... entries) throws IOException {
        List<Object> fields = new ArrayList<>(List.of("RiftServers", target, entries.length));
        for (Object[] entry : entries) {
            fields.addAll(List.of(entry));
        }
        return utf(fields.toArray());
    }

    private static byte[] inGroupReply(String player, String group, boolean member) throws IOException {
        return utf("RiftInGroup", player, group, new byte[] {(byte) (member ? 1 : 0)});
    }

    @Test
    void listsRiftServers() throws Exception {
        CompletableFuture<List<RiftServer>> all = messenger.riftServers(BungeeMessenger.ALL);
        assertEquals(List.of("RiftServers", "ALL"), lastStrings(2));
        CompletableFuture<List<RiftServer>> lobby = messenger.riftServers("lobby");
        assertEquals(List.of("RiftServers", "lobby"), lastStrings(2));

        messenger.receive("Steve", riftServers("ALL",
                new Object[] {"Lobby", "", "running", 1, 50},
                new Object[] {"game-1", "game", "draining", 4, 0}));
        // Rift repeats the configured name, which may differ in case from the request.
        messenger.receive("Steve", riftServers("Lobby", new Object[] {"Lobby", "", "running", 1, 50}));

        RiftServer lobbyServer = new RiftServer("Lobby", "", RiftServer.State.RUNNING, 1, 50);
        assertEquals(List.of(lobbyServer, new RiftServer("game-1", "game", RiftServer.State.DRAINING, 4, 0)),
                all.get(1, TimeUnit.SECONDS));
        assertEquals(List.of(lobbyServer), lobby.get(1, TimeUnit.SECONDS));

        assertTrue(lobbyServer.acceptsPlayers());
        assertEquals(OptionalInt.of(50), lobbyServer.limit());
        RiftServer game = all.get().get(1);
        assertFalse(game.acceptsPlayers());
        assertEquals(OptionalInt.empty(), game.limit());
    }

    @Test
    void riftServersForAnEmptyGroupIsAnEmptyList() throws Exception {
        CompletableFuture<List<RiftServer>> game = messenger.riftServers("GAME");
        messenger.receive("Steve", riftServers("game"));
        assertEquals(List.of(), game.get(1, TimeUnit.SECONDS));
    }

    @Test
    void unknownServerStatesAcceptNoPlayers() throws Exception {
        CompletableFuture<List<RiftServer>> servers = messenger.riftServers("lobby");
        messenger.receive("Steve", riftServers("lobby", new Object[] {"lobby", "", "warming", 0, 0}));
        RiftServer server = servers.get(1, TimeUnit.SECONDS).getFirst();
        assertEquals(RiftServer.State.DRAINING, server.state());
        assertFalse(server.acceptsPlayers());
    }

    @Test
    void ignoresMalformedRiftServersReplies() {
        CompletableFuture<List<RiftServer>> servers = messenger.riftServers("lobby");
        assertDoesNotFail(() -> messenger.receive("Steve", utf("RiftServers", "lobby", -1)));
        assertDoesNotFail(() -> messenger.receive("Steve", utf("RiftServers", "lobby", 2, "lobby", "")));
        assertFalse(servers.isDone());
    }

    @Test
    void repeatedRiftServersQueriesCompleteInOrder() throws Exception {
        CompletableFuture<List<RiftServer>> first = messenger.riftServers("lobby");
        CompletableFuture<List<RiftServer>> second = messenger.riftServers("Lobby");
        messenger.receive("Steve", riftServers("Lobby", new Object[] {"Lobby", "", "running", 1, 0}));
        messenger.receive("Steve", riftServers("Lobby", new Object[] {"Lobby", "", "running", 2, 0}));
        assertEquals(1, first.get(1, TimeUnit.SECONDS).getFirst().players());
        assertEquals(2, second.get(1, TimeUnit.SECONDS).getFirst().players());
    }

    @Test
    void answersInGroupQueries() throws Exception {
        CompletableFuture<Boolean> staff = messenger.inGroup("alice", "staff");
        assertEquals(List.of("RiftInGroup", "alice", "staff"), lastStrings(3));
        CompletableFuture<Boolean> upperStaff = messenger.inGroup("alice", "Staff");

        // The player name ignores case, the group must match exactly.
        messenger.receive("Steve", inGroupReply("Alice", "staff", true));
        assertTrue(staff.get(1, TimeUnit.SECONDS));
        assertFalse(upperStaff.isDone());
        messenger.receive("Steve", inGroupReply("Alice", "Staff", false));
        assertFalse(upperStaff.get(1, TimeUnit.SECONDS));
    }

    @Test
    void repeatedInGroupQueriesCompleteInOrder() throws Exception {
        CompletableFuture<Boolean> first = messenger.inGroup("Alice", "vip");
        CompletableFuture<Boolean> second = messenger.inGroup("ALICE", "vip");
        messenger.receive("Steve", inGroupReply("Alice", "vip", false));
        messenger.receive("Steve", inGroupReply("Alice", "vip", true));
        assertFalse(first.get(1, TimeUnit.SECONDS));
        assertTrue(second.get(1, TimeUnit.SECONDS));
    }

    @Test
    void detectsRiftAndCachesTheAnswer() throws Exception {
        CompletableFuture<Boolean> rift = messenger.isRift();
        assertEquals(List.of("RiftServers", "ALL"), lastStrings(2));
        assertSame(rift, messenger.isRift());
        messenger.receive("Steve", riftServers("ALL"));
        assertTrue(rift.get(1, TimeUnit.SECONDS));
        int requests = sent.size();
        assertTrue(messenger.isRift().get(1, TimeUnit.SECONDS));
        assertEquals(requests, sent.size());
    }

    @Test
    void anUnansweredProbeMeansNoRift() throws Exception {
        assertFalse(messenger.isRift().get(2, TimeUnit.SECONDS));
        int requests = sent.size();
        assertFalse(messenger.isRift().get(1, TimeUnit.SECONDS));
        assertEquals(requests, sent.size());
    }

    @Test
    void probingWithoutACarrierIsRetried() throws Exception {
        online = null;
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> messenger.isRift().get(1, TimeUnit.SECONDS));
        assertInstanceOf(NoCarrierException.class, error.getCause());
        online = "Steve";
        CompletableFuture<Boolean> rift = messenger.isRift();
        messenger.receive("Steve", riftServers("ALL"));
        assertTrue(rift.get(1, TimeUnit.SECONDS));
    }

    @Test
    void unansweredQueriesTimeOut() {
        CompletableFuture<String> missing = messenger.playerServer("Nobody");
        ExecutionException error = assertThrows(ExecutionException.class, () -> missing.get(2, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, error.getCause());
        // A late reply after the timeout is ignored.
        assertDoesNotFail(() -> messenger.receive("Steve", utf("GetPlayerServer", "Nobody", "lobby")));
    }

    @Test
    void requiresACarrier() {
        online = null;
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> messenger.servers().get(1, TimeUnit.SECONDS));
        assertInstanceOf(NoCarrierException.class, error.getCause());
        assertThrows(NoCarrierException.class, () -> messenger.message(BungeeMessenger.ALL, "hi"));
        assertTrue(sent.isEmpty());
    }

    @Test
    void forwardsPluginData() throws IOException {
        byte[] chat = "hello".getBytes(StandardCharsets.UTF_8);
        messenger.forward(BungeeMessenger.ALL, "MyChat", chat);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(sent.getLast().message()));
        assertEquals("Forward", in.readUTF());
        assertEquals("ALL", in.readUTF());
        assertEquals("MyChat", in.readUTF());
        byte[] data = new byte[in.readUnsignedShort()];
        in.readFully(data);
        assertArrayEquals(chat, data);
        assertEquals(0, in.available());

        messenger.forwardToPlayer("Alex", "MyChat", chat);
        assertArrayEquals(utf("ForwardToPlayer", "Alex", "MyChat", (short) chat.length, chat), sent.getLast().message());
        assertThrows(IllegalArgumentException.class, () -> messenger.forward("ALL", "big", new byte[70_000]));
    }

    @Test
    void deliversForwardedData() throws Exception {
        List<String> received = new ArrayList<>();
        AutoCloseable registration = messenger.onForward("MyChat",
                (carrier, data) -> received.add(carrier + ":" + new String(data, StandardCharsets.UTF_8)));
        messenger.receive("Steve", utf("MyChat", (short) 2, "hi".getBytes(StandardCharsets.UTF_8)));
        messenger.receive("Steve", utf("Other", (short) 1, new byte[] {1}));
        messenger.receive("Steve", new byte[] {0, 9, 1});
        assertEquals(List.of("Steve:hi"), received);
        registration.close();
        messenger.receive("Steve", utf("MyChat", (short) 2, "hi".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, received.size());
    }

    @Test
    void encodesMessagesAndKicks() throws IOException {
        messenger.message("Alex", "§aHello");
        assertEquals(List.of("Message", "Alex", "§aHello"), lastStrings(3));
        messenger.messageRaw(BungeeMessenger.ALL, "{\"text\":\"Hi\"}");
        assertEquals(List.of("MessageRaw", "ALL", "{\"text\":\"Hi\"}"), lastStrings(3));
        messenger.kick("Alex", "Bye");
        assertEquals(List.of("KickPlayer", "Alex", "Bye"), lastStrings(3));
        messenger.kickRaw("Alex", "{\"text\":\"Bye\"}");
        assertEquals(List.of("KickPlayerRaw", "Alex", "{\"text\":\"Bye\"}"), lastStrings(3));
        assertThrows(IllegalArgumentException.class, () -> messenger.message("Alex", "x".repeat(40_000)));
    }

    @Test
    void parsesUuids() throws IOException {
        assertEquals(new UUID(0, 0), BungeeMessenger.parseUuid("0".repeat(32)));
        assertThrows(IOException.class, () -> BungeeMessenger.parseUuid("abc"));
        assertThrows(IOException.class, () -> BungeeMessenger.parseUuid("g".repeat(32)));
    }

    private interface Action {
        void run() throws Exception;
    }

    private static void assertDoesNotFail(Action action) {
        try {
            action.run();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
