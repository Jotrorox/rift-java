package com.jotrorox.rift.paper;

import com.jotrorox.rift.Message;
import com.jotrorox.rift.MessageHandler;
import com.jotrorox.rift.RiftClient;
import com.jotrorox.rift.RiftConnection;
import com.jotrorox.rift.RiftException;
import com.jotrorox.rift.control.RiftControl;
import com.jotrorox.rift.paper.bungee.BungeeChannel;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.event.EventHandler;
import org.bukkit.event.HandlerList;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.plugin.Plugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

/**
 * Rift for a Paper plugin: one reconnecting connection whose lifetime follows
 * the plugin, with handlers that run on the server thread.
 *
 * <pre>{@code
 * public final class ChatPlugin extends JavaPlugin {
 *     private RiftPaper rift;
 *
 *     public void onEnable() {
 *         saveDefaultConfig();
 *         rift = RiftPaper.connect(this);
 *         rift.subscribe("network.chat", message ->
 *                 getServer().broadcast(Component.text(message.payloadAsString())));
 *     }
 *
 *     public void onDisable() {
 *         rift.close();
 *     }
 * }
 * }</pre>
 *
 * <p>Nothing here blocks the server thread: connecting happens in the
 * background, and the {@code *Async} methods run network calls off-thread.
 * The connection also closes on its own when the plugin is disabled.
 */
public final class RiftPaper implements AutoCloseable {
    private final Plugin plugin;
    private final RiftConnection connection;
    private final ExecutorService io;
    private final Listener disableListener;
    private BungeeChannel bungee;

    private RiftPaper(Plugin plugin, RiftConnection.Connector connector) {
        this.plugin = plugin;
        this.io = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, plugin.getName() + "-rift");
            thread.setDaemon(true);
            return thread;
        });
        this.connection = RiftConnection.builder(connector)
                .callbackExecutor(serverThread(plugin))
                .listener(new RiftConnection.Listener() {
                    @Override
                    public void connected(RiftClient client) {
                        plugin.getLogger().info("Connected to Rift");
                    }

                    @Override
                    public void disconnected(Throwable cause) {
                        plugin.getLogger().warning("Lost the connection to Rift; reconnecting");
                    }

                    @Override
                    public void connectFailed(IOException error, Duration retryIn) {
                        plugin.getLogger().warning("Could not connect to Rift (" + error.getMessage()
                                + "); retrying in " + retryIn.toSeconds() + "s");
                    }
                })
                .start();
        this.disableListener = new Listener() {
            @EventHandler
            public void onDisable(PluginDisableEvent event) {
                if (event.getPlugin() == plugin) {
                    close();
                }
            }
        };
        plugin.getServer().getPluginManager().registerEvents(disableListener, plugin);
    }

    /** Connects with the {@code rift} section of the plugin's {@code config.yml}. */
    public static RiftPaper connect(Plugin plugin) {
        ConfigurationSection section = plugin.getConfig().getConfigurationSection("rift");
        if (section == null) {
            throw new IllegalArgumentException("config.yml of " + plugin.getName() + " has no rift section");
        }
        return connect(plugin, RiftSettings.from(section, plugin.getDataFolder().toPath()));
    }

    /** Connects with explicit settings. */
    public static RiftPaper connect(Plugin plugin, RiftSettings settings) {
        RiftClient.Builder builder;
        try {
            builder = settings.clientBuilder();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read " + settings.certificate(), e);
        }
        return connect(plugin, builder::connect);
    }

    /** Connects with a custom connector, for example a fully configured {@link RiftClient.Builder}. */
    public static RiftPaper connect(Plugin plugin, RiftConnection.Connector connector) {
        return new RiftPaper(plugin, connector);
    }

    /** The underlying reconnecting connection. */
    public RiftConnection connection() {
        return connection;
    }

    /** Completes on the first successful connection. */
    public CompletableFuture<RiftClient> ready() {
        return connection.ready();
    }

    public boolean isConnected() {
        return connection.isConnected();
    }

    /** The current client, if connected. Do not call its blocking methods on the server thread. */
    public Optional<RiftClient> client() {
        return connection.client();
    }

    /**
     * Subscribes for the plugin's lifetime, restoring the subscription after
     * reconnects. The handler runs on the server thread.
     */
    public RiftConnection.Registration subscribe(String pattern, MessageHandler handler) {
        return connection.subscribe(pattern, handler);
    }

    /** Like {@link #subscribe(String, MessageHandler)}, sharing messages within a queue group. */
    public RiftConnection.Registration subscribe(String pattern, String queueGroup, MessageHandler handler) {
        return connection.subscribe(pattern, queueGroup, handler);
    }

    /** Publishes off the server thread; the future fails while Rift is unreachable. */
    public CompletableFuture<Void> publishAsync(String subject, byte[] payload) {
        return run(() -> {
            connection.publish(subject, payload);
            return null;
        });
    }

    /** Publishes UTF-8 text off the server thread. */
    public CompletableFuture<Void> publishAsync(String subject, String payload) {
        return publishAsync(subject, payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Sends a request off the server thread. Chain with {@link #onServerThread()} to touch game state. */
    public CompletableFuture<Message> requestAsync(String subject, byte[] payload, Duration timeout) {
        return run(() -> connection.request(subject, payload, timeout));
    }

    /**
     * Runs a control operation off the server thread.
     *
     * <pre>{@code
     * rift.controlAsync(control -> control.status())
     *     .thenAcceptAsync(status -> sender.sendMessage(status.playersOnline() + " online"), rift.onServerThread());
     * }</pre>
     */
    public <T> CompletableFuture<T> controlAsync(ControlCall<T> call) {
        return run(() -> call.apply(connection.control()));
    }

    /** An executor that runs tasks on the server thread, for chaining futures. */
    public Executor onServerThread() {
        return serverThread(plugin);
    }

    /**
     * The BungeeCord plugin channel, which Rift answers when
     * {@code network = { bungeecord = true }} is configured. It works without
     * the QUIC messaging listener but needs an online player to carry messages.
     */
    public synchronized BungeeChannel bungee() {
        if (bungee == null) {
            bungee = BungeeChannel.register(plugin);
        }
        return bungee;
    }

    @Override
    public void close() {
        HandlerList.unregisterAll(disableListener);
        connection.close();
        io.shutdownNow();
        synchronized (this) {
            if (bungee != null) {
                bungee.close();
            }
        }
    }

    /** A control operation. */
    @FunctionalInterface
    public interface ControlCall<T> {
        T apply(RiftControl control) throws IOException;
    }

    @FunctionalInterface
    private interface IoCall<T> {
        T call() throws IOException;
    }

    private <T> CompletableFuture<T> run(IoCall<T> call) {
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            io.execute(() -> {
                try {
                    future.complete(call.call());
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (RuntimeException e) {
            future.completeExceptionally(new RiftException("Rift connection is closed", e));
        }
        return future;
    }

    private static Executor serverThread(Plugin plugin) {
        return task -> {
            if (!plugin.isEnabled()) {
                return;
            }
            try {
                // The global region scheduler is the server thread on Paper and the global region on Folia.
                plugin.getServer().getGlobalRegionScheduler().execute(plugin, task);
            } catch (RuntimeException e) {
                plugin.getLogger().log(Level.FINE, "Dropped a Rift message during shutdown", e);
            }
        };
    }
}
