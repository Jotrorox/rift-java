# rift-java

Java libraries for [Rift](https://github.com/Jotrorox/rift), the Minecraft proxy.
Use them from Paper plugins, Velocity plugins or any JVM service to publish and
subscribe on Rift's message bus, make requests, work with retained streams and
operate the proxy.

| Module | Use it for |
| --- | --- |
| `rift-client` | The messaging client: pub/sub, request/reply, retained streams, `rift.control` and reconnects. No server API dependency. |
| `rift-paper` | Paper plugins: config-driven setup, server-thread handlers, plugin lifecycle and a typed BungeeCord channel. Includes `rift-client`. |

The client speaks Rift's [QUIC messaging protocol](https://jotrorox.github.io/rift/docs/messaging-protocol/)
through [Kwik](https://github.com/ptrd/kwik), a QUIC implementation in pure
Java, so plugins ship no native libraries. Java 21 or newer is required.

## Configure Rift

Enable Rift's messaging listener and give each server its own principal. The
certificate must contain the DNS name clients verify (here `localhost`) as a
subject alternative name.

```lua
rift.config.messaging = {
    listen = "0.0.0.0:4222",
    certificate = "messaging-cert.pem",
    private_key = "messaging-key.pem",
    principals = {
        {
            name = "paper",
            token_env = "RIFT_PAPER_TOKEN",
            publish = { "network.>", "_INBOX.>" },
            subscribe = { "network.>", "rift.events.>" },
        },
    },
}
```

Tokens need 32 to 1024 bytes. Grant publish on `_INBOX.>` only to servers that
answer requests, and keep subscribe grants narrow: never grant subscribe on
`_INBOX.>`. See Rift's [messaging guide](https://jotrorox.github.io/rift/docs/messaging/)
for streams and control principals.

## Paper plugins

Add `rift-paper` and shade it into the plugin jar. The artifacts are not on
Maven Central yet; run `./gradlew publishToMavenLocal` in this repository and
add `mavenLocal()` to your repositories.

```kotlin
plugins {
    java
    id("com.gradleup.shadow") version "9.6.1"
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.11-R0.1-SNAPSHOT")
    implementation("com.jotrorox.rift:rift-paper:0.1.0-SNAPSHOT") {
        exclude(group = "com.google.code.gson") // Paper already provides Gson.
    }
}

tasks.shadowJar {
    relocate("tech.kwik", "com.example.myplugin.libs.kwik")
    relocate("at.favre.lib.hkdf", "com.example.myplugin.libs.hkdf")
}
```

Put the connection settings in the plugin's `config.yml` and copy Rift's
certificate into the plugin's data folder:

```yaml
rift:
  host: 10.0.0.2
  port: 4222
  server-name: localhost
  certificate: messaging-cert.pem
  token-env: RIFT_PAPER_TOKEN
```

Set `RIFT_PAPER_TOKEN` in the server's environment. Then connect when the
plugin enables:

```java
public final class NetworkChat extends JavaPlugin implements Listener {
    private RiftPaper rift;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        rift = RiftPaper.connect(this);
        // Runs on the server thread and survives Rift restarts.
        rift.subscribe("network.chat", message ->
                getServer().broadcast(Component.text(message.payloadAsString())));
        getServer().getPluginManager().registerEvents(this, this);
    }

    @EventHandler
    public void onChat(AsyncChatEvent event) {
        String text = event.getPlayer().getName() + ": "
                + PlainTextComponentSerializer.plainText().serialize(event.message());
        rift.publishAsync("network.chat", text);
    }

    @Override
    public void onDisable() {
        rift.close();
    }
}
```

`RiftPaper` connects in the background, reconnects with backoff, restores
subscriptions after reconnecting and closes itself when the plugin is disabled.
Subscription handlers run on the server thread (the global region on Folia).
Messages published while Rift is unreachable are lost: `publishAsync` fails
instead of queueing.

Run requests and control commands with the async helpers, then switch back to
the server thread to touch game state:

```java
rift.controlAsync(control -> control.status())
        .thenAcceptAsync(status -> sender.sendMessage(status.playersOnline() + " players on the network"),
                rift.onServerThread());

rift.requestAsync("network.balance", player.getName().getBytes(UTF_8), Duration.ofSeconds(2))
        .thenAcceptAsync(reply -> player.sendMessage("Balance: " + reply.payloadAsString()),
                rift.onServerThread());
```

### BungeeCord channel

With `rift.config.network = { bungeecord = true }`, Rift answers the classic
BungeeCord plugin channel over player connections. `BungeeChannel` wraps it
with typed requests and matches replies to queries. It does not need the
messaging listener.

```java
BungeeChannel bungee = rift.bungee(); // or BungeeChannel.register(plugin)

bungee.connect(player, "survival");
bungee.playerCount(BungeeChannel.ALL).thenAccept(count -> player.sendMessage(count + " online"));
bungee.playerServer("Alex").thenAccept(server -> player.sendMessage("Alex is on " + server));

bungee.forward(BungeeChannel.ALL, "MyChat", "hello".getBytes(UTF_8));
bungee.onForward("MyChat", (carrier, data) -> getLogger().info(new String(data, UTF_8)));
```

Plugin messages need an online player to carry them. Without one, queries fail
with `NoCarrierException`. Rift sends no reply for unknown players or servers,
so those queries fail with a `TimeoutException` after five seconds (change it
with `bungee.timeout(...)`). Replies arrive on the server thread.

Rift also answers two requests of its own. `riftServers` lists the running
servers of a server, service group or `ALL` with their group, state and
capacity in one reply, and `inGroup` checks a player's permission group:

```java
bungee.riftServers("lobby").thenAccept(servers -> {
    for (RiftServer server : servers) {
        if (server.acceptsPlayers()) { // false while draining
            String capacity = server.limit().isPresent()
                    ? server.players() + "/" + server.limit().getAsInt()
                    : server.players() + " online";
            menu.add(server.name(), capacity);
        }
    }
});
bungee.inGroup(player.getName(), "staff").thenAccept(staff -> { /* ... */ });
```

A service group without running instances gives an empty list. Target names
ignore case except for `ALL`; group names in `inGroup` must match exactly.
These requests get no reply, and so fail with a `TimeoutException`, for an
unknown target, for a player who is not online, for `inGroup` when Rift runs
without extensions using `api_version = 2`, and behind Velocity or BungeeCord.
Call `bungee.isRift()` first to choose between Rift and plain BungeeCord
behavior: it waits at most one second and caches the answer.

## The client

`rift-client` works on any JVM. Keep one client open instead of reconnecting
for each message. Its methods block, so call them off the game thread.

```java
RiftClient client = RiftClient.builder()
        .address("10.0.0.2", 4222)
        .serverName("localhost")
        .trustCertificates(Path.of("messaging-cert.pem"))
        .tokenFromEnvironment("RIFT_PAPER_TOKEN")
        .connect();

// Each subscription has its own stream and delivery thread.
client.subscribe("network.player.*", message ->
        System.out.println(message.subject() + ": " + message.payloadAsString()));

// Waits until the broker accepted the message.
client.publish("network.player.join", "Steve");

// Pipelined publishing for high rates; flush() confirms everything before it.
try (Publisher publisher = client.publisher()) {
    for (int score = 0; score < 1000; score++) {
        publisher.publish("network.score.steve", Integer.toString(score));
    }
    publisher.flush();
}
```

Subscriptions without a handler buffer messages for `next()` and
`poll(Duration)`. Pass a queue group to share work between subscribers:

```java
try (Subscription jobs = client.subscribe("network.jobs", "workers", null)) {
    Message job = jobs.poll(Duration.ofSeconds(5));
}
```

Live delivery is at most once. Rift drops messages without a matching
subscriber, and it disconnects subscribers that fall too far behind.
`Subscription.closed()` completes when a subscription ends.

### Reconnecting

`RiftConnection` reconnects with exponential backoff and restores
subscriptions after Rift restarts or slow-consumer disconnects. `RiftPaper` is
built on it.

```java
RiftConnection rift = RiftConnection.builder(builder::connect)
        .callbackExecutor(mainThreadExecutor) // optional
        .start();
rift.subscribe("network.chat", message -> show(message.payloadAsString()));
rift.ready().join();
rift.publish("network.chat", "hello");
```

### Request and reply

Rift creates a private inbox for every request, so requesters need no inbox
subscribe grant. Responders answer on the message's reply subject and need
publish access to `_INBOX.>`.

```java
client.subscribe("network.time", message -> client.publish(message.reply(), "noon"));

Message reply = client.request("network.time", new byte[0], Duration.ofSeconds(2));
```

A request without responders fails with `RiftRejectedException`.

### Deciding logins and transfers

Rift can ask a JVM service whether a player may log in, which server they start
on and whether a transfer may go ahead. Configure the hook in Rift's
[`extensions.decisions`](https://jotrorox.github.io/rift/docs/extensions/#decisions-answered-over-the-message-bus),
then answer it with `decide` on a `RiftConnection` (or `RiftPaper`):

```java
RiftConnection rift = RiftConnection.start(builder::connect);
rift.decide("relaxoban.login", request -> bans.activeBan(request.uuid())
        .thenApply(ban -> ban.map(b -> Decision.deny(b.message())).orElse(Decision.proceed())));
```

The decider is subscribed again after every reconnect. `RiftClient.decide`
exists too, but it stops answering when that one connection ends. Rift then
applies `on_no_responders` to every login until the decider restarts, so use it
only where something else handles reconnects.

The handler gets a `DecisionRequest` (hook, UUID, name, IP, hostname, current
server, transfer target and reason) and returns a `CompletionStage<Decision>`:
`Decision.proceed()`, `Decision.deny(reason)` or, for `initial_server` only,
`Decision.server(name)`. Complete it from your own executor; the handler runs on
the subscription's delivery thread, even with a callback executor or on Paper.
`decide` joins the queue group `deciders`, so several instances share the load.
If the handler fails, no reply is sent and Rift applies its timeout policy.
Decisions must not have side effects: Rift may have timed out and moved on while
the request was still being processed.

The decider needs subscribe access to the subject and publish access to
`_INBOX.>`. Run it independently of the game servers. If it runs only on a
lobby that Rift starts on demand, the first login is denied, so the lobby never
starts.

### Retained streams

Streams keep records until they are acknowledged, for work that must not be
lost. Configure them under `messaging.streams` in Rift.

```java
client.streamPublish("jobs", "network.jobs.build", payload);

StreamConsumer consumer = client.consumer("jobs", "builder",
        ConsumerConfig.builder("network.jobs.>").ackWait(Duration.ofSeconds(30)).build());
for (Delivery delivery : consumer.fetch(32)) {
    process(delivery.message());
    consumer.ack(delivery);
}
```

Delivery is at least once: records that are not acknowledged before the ack
wait runs out are delivered again. Consumer names are scoped to the principal;
reopen a consumer with the same name and settings to resume it.

### Operating the proxy

Principals with `control = true` and a publish grant for `rift.control.*` can
operate Rift:

```java
RiftControl control = client.control();
ProxyStatus status = control.status();
control.maintenance(true);
control.drain("survival", true);
control.transfer("Steve", "lobby");
control.reload();
```

When Rift runs game servers on more hosts with `rift node`, `nodes()` lists
them with their reachability, labels and capacity:

```java
for (NodeStatus node : control.nodes()) {
    System.out.printf("%s %s %d/%d instances%n", node.name(), node.state(), node.instances(), node.maxInstances());
}
```

Each entry of `status().managedServers()` carries its `node` (`local` for the
proxy host); servers on an unreachable node report the state `unknown`.

Refused commands throw `ControlException`. A command that times out may still
have run, so check `status()` before retrying it. Rift reports operational
events on `rift.events.*`:

```java
client.subscribe(RiftEvents.ALL, message -> RiftEvents.parse(message).ifPresent(event -> {
    if (event instanceof RiftEvents.Reload reload) {
        System.out.println("Rift reloaded revision " + reload.revision());
    }
}));
```

`RiftEvents.PLAYERS` reports joins, server switches and quits as
`RiftEvents.Player`, for online status and friend notifications. Any principal
with a subscribe grant can follow it; it needs no `control` flag.

## Errors

All failures are `IOException`s:

- `RiftPermissionException`: the principal lacks a grant, or authentication failed.
- `RiftRejectedException`: Rift refused the operation, for example an unknown stream or a request without responders.
- `RiftTimeoutException`: no answer before the deadline (30 seconds by default, set with `operationTimeout`).
- `ControlException`: a control command was refused.

A failed publication may still have reached the broker. If retries must not
create duplicates, include your own message IDs.

## Limits

- The server certificate must name the host as a DNS subject alternative
  name. Kwik does not check IP address entries, so to connect to an IP address,
  set `serverName` to a DNS name the certificate contains.
- Payloads are limited to 1 MiB and subjects to 1024 UTF-8 bytes.
- Requests time out after 1 ms to 120 seconds; fetches return at most 256 records.

## Development

```sh
./gradlew build
```

The tests include integration tests against a real Rift binary. They use
`RIFT_BINARY`, or a Rift checkout next to this repository
(`../rift/target/{release,debug}/rift`), and are skipped when neither exists.
CI runs them against the latest Rift nightly.

[BSD-2-Clause](LICENSE)
