package com.jotrorox.rift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Runs a real Rift binary with a QUIC messaging listener for integration tests.
 *
 * <p>The binary comes from the {@code rift.binary} system property (set from the
 * {@code RIFT_BINARY} environment variable by the build) or from a sibling
 * {@code rift} checkout.
 */
final class RiftProcess implements AutoCloseable {
    static final String PAPER_TOKEN = "paper-token-0123456789abcdefghijklmnopqrstuv";
    static final String OPERATOR_TOKEN = "operator-token-0123456789abcdefghijklmnopqrs";
    static final String LIMITED_TOKEN = "limited-token-0123456789abcdefghijklmnopqrstu";

    private final Process process;
    private final Path directory;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final int messagingPort;
    final Path certificate;

    private RiftProcess(Process process, Path directory, int messagingPort, Path certificate) {
        this.process = process;
        this.directory = directory;
        this.messagingPort = messagingPort;
        this.certificate = certificate;
        Thread pump = new Thread(() -> {
            try (InputStream in = process.getInputStream()) {
                in.transferTo(new SynchronizedSink(output));
            } catch (IOException ignored) {
                // Process ended.
            }
        }, "rift-output");
        pump.setDaemon(true);
        pump.start();
    }

    /** The Rift binary to test against, or {@code null} when none is available. */
    static Path binary() {
        String configured = System.getProperty("rift.binary", "");
        if (!configured.isBlank()) {
            return Path.of(configured);
        }
        for (String candidate : List.of("../../rift/target/release/rift", "../../rift/target/debug/rift",
                "../rift/target/release/rift", "../rift/target/debug/rift")) {
            Path path = Path.of(candidate).toAbsolutePath().normalize();
            if (Files.isExecutable(path)) {
                return path;
            }
        }
        return null;
    }

    static RiftProcess start(Path binary) throws Exception {
        Path directory = Files.createTempDirectory("rift-java-it");
        Path certificate = directory.resolve("messaging-cert.pem");
        Path key = directory.resolve("messaging-key.pem");
        createCertificate(directory, certificate, key);

        int gamePort;
        try (ServerSocket socket = new ServerSocket(0)) {
            gamePort = socket.getLocalPort();
        }
        int messagingPort;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            messagingPort = socket.getLocalPort();
        }
        String config = """
                rift.config.listeners = { public = "127.0.0.1:%d" }
                rift.config.backends = { lobby = "127.0.0.1:1", survival = "127.0.0.1:2" }
                rift.config.routes = { public = "lobby" }
                rift.config.messaging = {
                    listen = "127.0.0.1:%d",
                    certificate = "messaging-cert.pem",
                    private_key = "messaging-key.pem",
                    subscriptions = { { subject = "lua.echo" } },
                    principals = {
                        {
                            name = "paper",
                            token_env = "RIFT_TEST_PAPER_TOKEN",
                            publish = { "game.>", "lua.>", "_INBOX.>" },
                            subscribe = { "game.>", "rift.events.>", "_INBOX.paper.>" },
                        },
                        {
                            name = "operator",
                            token_env = "RIFT_TEST_OPERATOR_TOKEN",
                            publish = { "rift.control.*" },
                            subscribe = { "rift.events.>" },
                            control = true,
                        },
                        {
                            name = "limited",
                            token_env = "RIFT_TEST_LIMITED_TOKEN",
                            publish = { "game.chat" },
                            subscribe = { "game.chat" },
                        },
                    },
                    streams = {
                        jobs = { subjects = { "game.jobs.>" }, max_messages = 1000 },
                    },
                }
                rift.config.on_message = function(message)
                    if message.subject == "lua.echo" and message.reply then
                        rift.publish(message.reply, "echo:" .. message.payload)
                    end
                end
                """.formatted(gamePort, messagingPort);
        Files.writeString(directory.resolve("rift.lua"), config);

        ProcessBuilder builder = new ProcessBuilder(binary.toString(), "--config", "rift.lua")
                .directory(directory.toFile())
                .redirectErrorStream(true);
        Map<String, String> environment = builder.environment();
        environment.put("RIFT_TEST_PAPER_TOKEN", PAPER_TOKEN);
        environment.put("RIFT_TEST_OPERATOR_TOKEN", OPERATOR_TOKEN);
        environment.put("RIFT_TEST_LIMITED_TOKEN", LIMITED_TOKEN);
        RiftProcess rift = new RiftProcess(builder.start(), directory, messagingPort, certificate);
        rift.awaitReady(gamePort);
        return rift;
    }

    RiftClient.Builder client(String token) throws IOException {
        return RiftClient.builder()
                .address("127.0.0.1", messagingPort)
                .serverName("localhost")
                .trustCertificates(certificate)
                .connectTimeout(Duration.ofSeconds(10))
                .token(token);
    }

    String output() {
        synchronized (output) {
            return output.toString(StandardCharsets.UTF_8);
        }
    }

    private void awaitReady(int gamePort) throws Exception {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException("rift exited early:\n" + output());
            }
            try (java.net.Socket ignored = new java.net.Socket("127.0.0.1", gamePort)) {
                // The game listener binds after the messaging listener; give QUIC a moment as well.
                Thread.sleep(200);
                return;
            } catch (IOException notYet) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("rift did not start:\n" + output());
    }

    private static void createCertificate(Path directory, Path certificate, Path key) throws Exception {
        Path keystore = directory.resolve("messaging.p12");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        run(keytool, "-genkeypair", "-alias", "rift", "-keyalg", "EC", "-groupname", "secp256r1",
                "-sigalg", "SHA256withECDSA", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost",
                "-validity", "2", "-storetype", "PKCS12", "-keystore", keystore.toString(),
                "-storepass", "changeit", "-keypass", "changeit");
        run(keytool, "-exportcert", "-rfc", "-alias", "rift", "-keystore", keystore.toString(),
                "-storepass", "changeit", "-file", certificate.toString());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            store.load(in, "changeit".toCharArray());
        }
        Key privateKey = store.getKey("rift", "changeit".toCharArray());
        String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                .encodeToString(((PrivateKey) privateKey).getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        Files.writeString(key, pem);
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(new ArrayList<>(List.of(command))).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) {
            throw new IllegalStateException(String.join(" ", command) + " failed:\n" + output);
        }
    }

    @Override
    public void close() throws IOException {
        process.destroy();
        try {
            if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
        try (var files = Files.walk(directory)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    private static final class SynchronizedSink extends java.io.OutputStream {
        private final ByteArrayOutputStream target;

        private SynchronizedSink(ByteArrayOutputStream target) {
            this.target = target;
        }

        @Override
        public void write(int b) {
            synchronized (target) {
                target.write(b);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            synchronized (target) {
                target.write(b, off, len);
            }
        }
    }
}
