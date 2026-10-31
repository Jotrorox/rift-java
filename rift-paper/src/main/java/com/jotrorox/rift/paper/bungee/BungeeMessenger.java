package com.jotrorox.rift.paper.bungee;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * The BungeeCord plugin-message protocol as answered by Rift, independent of
 * the server API. Requests travel over a player connection (the "carrier");
 * {@link BungeeChannel} adapts this to Paper players.
 *
 * <p>Rift sends no reply for missing players or servers, so queries complete
 * exceptionally with a {@link java.util.concurrent.TimeoutException} after the
 * configured timeout. Queries without a carrier fail with
 * {@link NoCarrierException}.
 *
 * @param <P> the carrier type, such as a Paper {@code Player}
 */
public class BungeeMessenger<P> {
    /** The plugin channel name Paper plugins register. */
    public static final String CHANNEL = "BungeeCord";
    /** Target for every server ({@code PlayerCount}, {@code PlayerList}) or every player ({@code Message}). */
    public static final String ALL = "ALL";
    /** {@code Forward} target selecting every other server with players. */
    public static final String ONLINE = "ONLINE";
    /** Rift drops plugin messages larger than this. */
    public static final int MAX_MESSAGE_BYTES = 32_767;

    private static final Set<String> REPLY_SUBCHANNELS = Set.of("IP", "IPOther", "PlayerCount", "PlayerList",
            "GetServers", "GetServer", "GetPlayerServer", "UUID", "UUIDOther", "ServerIP");

    private final Carriers<P> carriers;
    private final Map<String, Deque<CompletableFuture<Object>>> pending = new HashMap<>();
    private final Map<String, List<BiConsumer<P, byte[]>>> forwardListeners = new ConcurrentHashMap<>();
    private volatile Duration timeout = Duration.ofSeconds(5);

    /** Access to the server's online players. */
    public interface Carriers<P> {
        /** Any online player able to carry a plugin message. */
        Optional<P> any();

        /** A stable identity of the player, used to match carrier-specific replies. */
        Object id(P player);

        /** Sends a {@code BungeeCord} plugin message over the player's connection. */
        void send(P player, byte[] message);
    }

    public BungeeMessenger(Carriers<P> carriers) {
        this.carriers = Objects.requireNonNull(carriers, "carriers");
    }

    /** How long queries wait for Rift's reply. Defaults to five seconds. */
    public void timeout(Duration timeout) {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        this.timeout = timeout;
    }

    /** Transfers {@code player} to {@code server}, subject to Rift's access and capacity checks. */
    public void connect(P player, String server) {
        carriers.send(player, encode("Connect", server));
    }

    /** Transfers the named player, wherever they are, to {@code server}. */
    public void connectOther(String playerName, String server) {
        send(encode("ConnectOther", playerName, server));
    }

    /** The address Rift sees for {@code player}. */
    public CompletableFuture<InetSocketAddress> address(P player) {
        return query(player, "IP|" + carriers.id(player), encode("IP"));
    }

    /** The address Rift sees for the named player. */
    public CompletableFuture<InetSocketAddress> playerAddress(String playerName) {
        return query("IPOther|" + key(playerName), encode("IPOther", playerName));
    }

    /** Players on {@code server}, or on the whole network for {@link #ALL}. */
    public CompletableFuture<Integer> playerCount(String server) {
        return query("PlayerCount|" + serverKey(server), encode("PlayerCount", server));
    }

    /** Names of the players on {@code server}, or on the whole network for {@link #ALL}. */
    public CompletableFuture<List<String>> playerList(String server) {
        return query("PlayerList|" + serverKey(server), encode("PlayerList", server));
    }

    /** Names of all servers configured in Rift. */
    public CompletableFuture<List<String>> servers() {
        return query("GetServers", encode("GetServers"));
    }

    /** The name Rift uses for the server {@code player} is connected to, usually this server. */
    public CompletableFuture<String> serverName(P player) {
        return query(player, "GetServer|" + carriers.id(player), encode("GetServer"));
    }

    /** The server name of this backend, using any online player as carrier. */
    public CompletableFuture<String> serverName() {
        Optional<P> carrier = carriers.any();
        return carrier.isPresent() ? serverName(carrier.get()) : CompletableFuture.failedFuture(new NoCarrierException());
    }

    /** The server the named player is connected to. */
    public CompletableFuture<String> playerServer(String playerName) {
        return query("GetPlayerServer|" + key(playerName), encode("GetPlayerServer", playerName));
    }

    /** The UUID Rift established for {@code player} at login. */
    public CompletableFuture<UUID> uuid(P player) {
        return query(player, "UUID|" + carriers.id(player), encode("UUID"));
    }

    /** The UUID of the named player. */
    public CompletableFuture<UUID> playerUuid(String playerName) {
        return query("UUIDOther|" + key(playerName), encode("UUIDOther", playerName));
    }

    /** The configured address of {@code server}; hostnames are returned unresolved. */
    public CompletableFuture<InetSocketAddress> serverAddress(String server) {
        return query("ServerIP|" + key(server), encode("ServerIP", server));
    }

    /**
     * Sends {@code data} to plugins on other servers, which receive it through
     * {@link #onForward}. Each target server needs an online player to receive it.
     *
     * @param target a server name, {@link #ALL} or {@link #ONLINE} (both: every other server with players)
     */
    public void forward(String target, String subchannel, byte[] data) {
        send(forwardMessage("Forward", target, subchannel, data));
    }

    /** Sends {@code data} to plugins on the server the named player is connected to. */
    public void forwardToPlayer(String playerName, String subchannel, byte[] data) {
        send(forwardMessage("ForwardToPlayer", playerName, subchannel, data));
    }

    /** Sends chat text with legacy {@code §} formatting to a player, or everyone for {@link #ALL}. */
    public void message(String playerName, String text) {
        send(encode("Message", playerName, text));
    }

    /** Sends a JSON text component to a player, or everyone for {@link #ALL}. */
    public void messageRaw(String playerName, String json) {
        send(encode("MessageRaw", playerName, json));
    }

    /** Disconnects a player with a text reason. */
    public void kick(String playerName, String reason) {
        send(encode("KickPlayer", playerName, reason));
    }

    /** Disconnects a player with a JSON text component as the reason. */
    public void kickRaw(String playerName, String json) {
        send(encode("KickPlayerRaw", playerName, json));
    }

    /**
     * Receives data that other servers sent with {@link #forward} or
     * {@link #forwardToPlayer} on {@code subchannel}. The handler gets the
     * player whose connection delivered the message and the data.
     */
    public AutoCloseable onForward(String subchannel, BiConsumer<P, byte[]> handler) {
        Objects.requireNonNull(handler, "handler");
        List<BiConsumer<P, byte[]>> handlers =
                forwardListeners.computeIfAbsent(subchannel, ignored -> new CopyOnWriteArrayList<>());
        handlers.add(handler);
        return () -> handlers.remove(handler);
    }

    /** Feeds an incoming {@code BungeeCord} plugin message received over {@code carrier}. */
    public void receive(P carrier, byte[] message) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(message))) {
            String subchannel = in.readUTF();
            if (REPLY_SUBCHANNELS.contains(subchannel) && handleReply(carrier, subchannel, in)) {
                return;
            }
            List<BiConsumer<P, byte[]>> handlers = forwardListeners.get(subchannel);
            if (handlers == null || handlers.isEmpty()) {
                return;
            }
            byte[] data = new byte[in.readUnsignedShort()];
            in.readFully(data);
            for (BiConsumer<P, byte[]> handler : handlers) {
                handler.accept(carrier, data);
            }
        } catch (IOException ignored) {
            // Malformed or unrelated messages are ignored, as Rift does.
        }
    }

    private boolean handleReply(P carrier, String subchannel, DataInputStream in) throws IOException {
        switch (subchannel) {
            case "IP" -> {
                return complete("IP|" + carriers.id(carrier),
                        InetSocketAddress.createUnresolved(in.readUTF(), in.readInt()));
            }
            case "IPOther" -> {
                String name = in.readUTF();
                return complete("IPOther|" + key(name), InetSocketAddress.createUnresolved(in.readUTF(), in.readInt()));
            }
            case "PlayerCount" -> {
                String server = in.readUTF();
                return complete("PlayerCount|" + serverKey(server), in.readInt());
            }
            case "PlayerList" -> {
                String server = in.readUTF();
                return complete("PlayerList|" + serverKey(server), list(in.readUTF()));
            }
            case "GetServers" -> {
                return complete("GetServers", list(in.readUTF()));
            }
            case "GetServer" -> {
                return complete("GetServer|" + carriers.id(carrier), in.readUTF());
            }
            case "GetPlayerServer" -> {
                String name = in.readUTF();
                return complete("GetPlayerServer|" + key(name), in.readUTF());
            }
            case "UUID" -> {
                return complete("UUID|" + carriers.id(carrier), parseUuid(in.readUTF()));
            }
            case "UUIDOther" -> {
                String name = in.readUTF();
                return complete("UUIDOther|" + key(name), parseUuid(in.readUTF()));
            }
            case "ServerIP" -> {
                String server = in.readUTF();
                String host = in.readUTF();
                return complete("ServerIP|" + key(server), InetSocketAddress.createUnresolved(host,
                        in.readUnsignedShort()));
            }
            default -> {
                return false;
            }
        }
    }

    private <T> CompletableFuture<T> query(String key, byte[] message) {
        Optional<P> carrier = carriers.any();
        if (carrier.isEmpty()) {
            return CompletableFuture.failedFuture(new NoCarrierException());
        }
        return query(carrier.get(), key, message);
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> query(P carrier, String key, byte[] message) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        synchronized (pending) {
            pending.computeIfAbsent(key, ignored -> new ArrayDeque<>()).add(future);
        }
        future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((value, error) -> forget(key, future));
        try {
            carriers.send(carrier, message);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
        return (CompletableFuture<T>) future;
    }

    private boolean complete(String key, Object value) {
        CompletableFuture<Object> future;
        synchronized (pending) {
            Deque<CompletableFuture<Object>> waiting = pending.get(key);
            if (waiting == null) {
                return false;
            }
            future = waiting.poll();
            if (waiting.isEmpty()) {
                pending.remove(key);
            }
        }
        if (future == null) {
            return false;
        }
        future.complete(value);
        return true;
    }

    private void forget(String key, CompletableFuture<Object> future) {
        synchronized (pending) {
            Deque<CompletableFuture<Object>> waiting = pending.get(key);
            if (waiting != null && waiting.remove(future) && waiting.isEmpty()) {
                pending.remove(key);
            }
        }
    }

    /** Fails all outstanding queries. */
    protected void cancelAll() {
        List<CompletableFuture<Object>> all;
        synchronized (pending) {
            all = pending.values().stream().flatMap(Deque::stream).toList();
            pending.clear();
        }
        all.forEach(future -> future.cancel(false));
        forwardListeners.clear();
    }

    private void send(byte[] message) {
        P carrier = carriers.any().orElseThrow(NoCarrierException::new);
        carriers.send(carrier, message);
    }

    /** Encodes a request: the subchannel followed by its string arguments. */
    public static byte[] encode(String subchannel, String... arguments) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(subchannel);
            for (String argument : arguments) {
                out.writeUTF(Objects.requireNonNull(argument, "argument"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return checkSize(bytes.toByteArray());
    }

    /** Encodes {@code Forward} or {@code ForwardToPlayer}. */
    public static byte[] forwardMessage(String kind, String target, String subchannel, byte[] data) {
        if (data.length > 0xFFFF) {
            throw new IllegalArgumentException("forwarded data must fit in an unsigned short length");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(kind);
            out.writeUTF(target);
            out.writeUTF(subchannel);
            out.writeShort(data.length);
            out.write(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return checkSize(bytes.toByteArray());
    }

    static UUID parseUuid(String hex) throws IOException {
        if (hex.length() != 32) {
            throw new IOException("UUID must have 32 hexadecimal digits");
        }
        try {
            return new UUID(Long.parseUnsignedLong(hex.substring(0, 16), 16),
                    Long.parseUnsignedLong(hex.substring(16), 16));
        } catch (NumberFormatException e) {
            throw new IOException("invalid UUID " + hex, e);
        }
    }

    private static List<String> list(String names) {
        return names.isEmpty() ? List.of() : Arrays.asList(names.split(", "));
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String serverKey(String server) {
        // ALL must be uppercase; server names ignore ASCII case.
        return server.equals(ALL) ? ALL : "server:" + key(server);
    }

    private static byte[] checkSize(byte[] message) {
        if (message.length > MAX_MESSAGE_BYTES) {
            throw new IllegalArgumentException("plugin message exceeds " + MAX_MESSAGE_BYTES + " bytes");
        }
        return message;
    }
}
