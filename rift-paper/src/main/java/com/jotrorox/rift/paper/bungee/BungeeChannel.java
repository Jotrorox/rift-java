package com.jotrorox.rift.paper.bungee;

import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.messaging.Messenger;
import org.bukkit.plugin.messaging.PluginMessageListener;

import java.util.Optional;

/**
 * Typed access to the {@code BungeeCord} plugin channel on Paper. Rift answers
 * it when {@code network = { bungeecord = true }} is set in its configuration;
 * the QUIC messaging listener is not required.
 *
 * <pre>{@code
 * BungeeChannel bungee = BungeeChannel.register(plugin);
 * bungee.connect(player, "survival");
 * bungee.playerCount(BungeeChannel.ALL)
 *       .thenAccept(count -> player.sendMessage(count + " players online"));
 * }</pre>
 *
 * <p>Replies arrive on the server thread, so futures complete there.
 */
public final class BungeeChannel extends BungeeMessenger<Player> implements PluginMessageListener, AutoCloseable {
    private final Plugin plugin;

    private BungeeChannel(Plugin plugin) {
        super(new PaperCarriers(plugin));
        this.plugin = plugin;
    }

    /** Registers the incoming and outgoing {@code BungeeCord} channel for {@code plugin}. */
    public static BungeeChannel register(Plugin plugin) {
        BungeeChannel channel = new BungeeChannel(plugin);
        Messenger messenger = plugin.getServer().getMessenger();
        messenger.registerOutgoingPluginChannel(plugin, CHANNEL);
        messenger.registerIncomingPluginChannel(plugin, CHANNEL, channel);
        return channel;
    }

    @Override
    public void onPluginMessageReceived(String channel, Player player, byte[] message) {
        if (CHANNEL.equals(channel)) {
            receive(player, message);
        }
    }

    /** Unregisters the channel and cancels outstanding queries. */
    @Override
    public void close() {
        Messenger messenger = plugin.getServer().getMessenger();
        messenger.unregisterIncomingPluginChannel(plugin, CHANNEL, this);
        messenger.unregisterOutgoingPluginChannel(plugin, CHANNEL);
        cancelAll();
    }

    private record PaperCarriers(Plugin plugin) implements Carriers<Player> {
        @Override
        public Optional<Player> any() {
            for (Player player : plugin.getServer().getOnlinePlayers()) {
                return Optional.of(player);
            }
            return Optional.empty();
        }

        @Override
        public Object id(Player player) {
            return player.getUniqueId();
        }

        @Override
        public void send(Player player, byte[] message) {
            player.sendPluginMessage(plugin, CHANNEL, message);
        }
    }
}
