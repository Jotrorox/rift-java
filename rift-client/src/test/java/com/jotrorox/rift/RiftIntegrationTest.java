package com.jotrorox.rift;

import com.jotrorox.rift.control.ControlException;
import com.jotrorox.rift.control.ProxyStatus;
import com.jotrorox.rift.control.RiftEvents;
import com.jotrorox.rift.stream.ConsumerConfig;
import com.jotrorox.rift.stream.Delivery;
import com.jotrorox.rift.stream.StoredRecord;
import com.jotrorox.rift.stream.StreamConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** End-to-end tests against a real Rift process; skipped when no Rift binary is available. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RiftIntegrationTest {
    private static final Duration WAIT = Duration.ofSeconds(5);

    private RiftProcess rift;

    @BeforeAll
    void startRift() throws Exception {
        Path binary = RiftProcess.binary();
        assumeTrue(binary != null, "no Rift binary; set RIFT_BINARY to run integration tests");
        rift = RiftProcess.start(binary);
    }

    @AfterAll
    void stopRift() throws Exception {
        if (rift != null) {
            rift.close();
        }
    }

    @Test
    void publishesToSubscribers() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect();
             Subscription subscription = client.subscribe("game.player.*")) {
            client.publish("game.player.join", "Steve");
            Message message = subscription.poll(WAIT);
            assertNotNull(message);
            assertEquals("game.player.join", message.subject());
            assertEquals("Steve", message.payloadAsString());
            assertNull(message.reply());
        }
    }

    @Test
    void deliversToHandlersBetweenClients() throws Exception {
        try (RiftClient receiver = rift.client(RiftProcess.PAPER_TOKEN).connect();
             RiftClient sender = rift.client(RiftProcess.PAPER_TOKEN).connect()) {
            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(3);
            receiver.subscribe("game.score.>", message -> {
                received.add(message.subject() + "=" + message.payloadAsString());
                latch.countDown();
            });
            try (Publisher publisher = sender.publisher()) {
                publisher.publish("game.score.a", "1");
                publisher.publish("game.score.b.c", "2");
                publisher.publish("game.score.a", "3");
                publisher.flush();
            }
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("game.score.a=1", "game.score.b.c=2", "game.score.a=3"), received);
        }
    }

    @Test
    void queueGroupsShareMessages() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect();
             Subscription first = client.subscribe("game.work", "workers", null);
             Subscription second = client.subscribe("game.work", "workers", null)) {
            try (Publisher publisher = client.publisher()) {
                for (int i = 0; i < 20; i++) {
                    publisher.publish("game.work", Integer.toString(i));
                }
                publisher.flush();
            }
            int total = 0;
            for (Subscription subscription : List.of(first, second)) {
                while (subscription.poll(Duration.ofMillis(300)) != null) {
                    total++;
                }
            }
            assertEquals(20, total);
        }
    }

    @Test
    void carriesBinaryAndLargePayloads() throws Exception {
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31);
        }
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect();
             Subscription subscription = client.subscribe("game.blob")) {
            client.publish("game.blob", payload);
            client.publish("game.blob", new byte[0]);
            assertArrayEquals(payload, subscription.poll(WAIT).payload());
            assertEquals(0, subscription.poll(WAIT).payload().length);
        }
    }

    @Test
    void requestsReachResponders() throws Exception {
        try (RiftClient responder = rift.client(RiftProcess.PAPER_TOKEN).connect();
             RiftClient requester = rift.client(RiftProcess.PAPER_TOKEN).connect()) {
            responder.subscribe("game.time", message -> {
                // Rift generated a private inbox for the reply.
                responder.publish(message.reply(), "noon");
            });
            Message reply = requester.request("game.time", new byte[0], WAIT);
            assertEquals("noon", reply.payloadAsString());
            assertEquals("noon", requester.requestAsync("game.time", new byte[0], WAIT)
                    .get(10, TimeUnit.SECONDS).payloadAsString());
        }
    }

    @Test
    void requestsReachLuaHandlers() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect()) {
            assertEquals("echo:hi", client.request("lua.echo", "hi", WAIT).payloadAsString());
        }
    }

    @Test
    void manualRepliesUseInboxSubjects() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect();
             Subscription inbox = client.subscribe("_INBOX.paper.manual")) {
            client.subscribe("game.ping", message -> client.publish(message.reply(), "pong"));
            client.publish("game.ping", "_INBOX.paper.manual", new byte[0]);
            assertEquals("pong", inbox.poll(WAIT).payloadAsString());
        }
    }

    @Test
    void requestWithoutResponderFails() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect()) {
            assertThrows(RiftRejectedException.class,
                    () -> client.request("game.nobody", new byte[0], Duration.ofMillis(200)));
        }
    }

    @Test
    void rejectsWrongTokens() {
        assertThrows(RiftException.class,
                () -> rift.client("wrong-token-0123456789abcdefghijklmnopqrstuv").connect());
    }

    @Test
    void rejectsUntrustedCertificates() throws Exception {
        RiftClient.Builder builder = rift.client(RiftProcess.PAPER_TOKEN)
                .trustCertificates(List.of(otherCertificate()))
                .connectTimeout(Duration.ofSeconds(3));
        assertThrows(RiftException.class, builder::connect);
    }

    @Test
    void enforcesPermissions() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.LIMITED_TOKEN).connect()) {
            assertThrows(RiftPermissionException.class, () -> client.subscribe("game.>"));
            assertThrows(RiftPermissionException.class, () -> client.publish("game.other", "x"));
            // The shared publisher recovers after a denied publication.
            try (Subscription chat = client.subscribe("game.chat")) {
                client.publish("game.chat", "hello");
                assertEquals("hello", chat.poll(WAIT).payloadAsString());
            }
            Publisher publisher = client.publisher();
            publisher.publish("game.other", "x");
            assertThrows(RiftPermissionException.class, publisher::flush);
            assertFalse(publisher.isUsable());
            assertThrows(RiftPermissionException.class, () -> client.control().status());
        }
    }

    @Test
    void retainedStreamsReplayAndAcknowledge() throws Exception {
        try (RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect()) {
            StoredRecord first = client.streamPublish("jobs", "game.jobs.build", "a".getBytes(StandardCharsets.UTF_8));
            StoredRecord second = client.streamPublish("jobs", "game.jobs.build", "b".getBytes(StandardCharsets.UTF_8));
            assertEquals(first.sequence() + 1, second.sequence());

            ConsumerConfig config = ConsumerConfig.builder("game.jobs.>")
                    .startSequence(first.sequence())
                    .ackWait(Duration.ofMillis(300))
                    .build();
            StreamConsumer consumer = client.consumer("jobs", "builder", config);
            List<Delivery> deliveries = consumer.fetch(10);
            assertEquals(2, deliveries.size());
            assertEquals("a", deliveries.get(0).message().payloadAsString());
            assertEquals(1, deliveries.get(0).deliveryCount());
            consumer.ack(deliveries.get(0));

            Thread.sleep(600);
            List<Delivery> again = consumer.fetch(10);
            assertEquals(1, again.size());
            assertEquals(second.sequence(), again.get(0).sequence());
            assertTrue(again.get(0).isRedelivery());
            consumer.ack(again.get(0));
            assertTrue(consumer.fetch(10).isEmpty());

            assertThrows(RiftRejectedException.class,
                    () -> client.streamPublish("missing", "game.jobs.x", new byte[0]));
        }
    }

    @Test
    void controlsTheProxy() throws Exception {
        try (RiftClient operator = rift.client(RiftProcess.OPERATOR_TOKEN).connect();
             Subscription events = operator.subscribe(RiftEvents.CONTROL)) {
            ProxyStatus status = operator.control().status();
            assertFalse(status.maintenance());
            assertEquals(0, status.playersOnline());
            assertTrue(status.backend("lobby").isPresent());

            assertTrue(operator.control().maintenance(true));
            assertTrue(operator.control().status().maintenance());
            assertFalse(operator.control().maintenance(false));

            assertTrue(operator.control().drain("survival", true));
            assertTrue(operator.control().status().backend("survival").orElseThrow().draining());
            assertFalse(operator.control().drain("survival", false));

            ControlException unknown = assertThrows(ControlException.class,
                    () -> operator.control().drain("missing", true));
            assertEquals("drain", unknown.command());
            assertThrows(ControlException.class, () -> operator.control().transfer("Nobody", "lobby"));

            RiftEvents.Event event = RiftEvents.parse(events.poll(WAIT)).orElseThrow();
            RiftEvents.Control control = assertInstanceOf(RiftEvents.Control.class, event);
            assertEquals("maintenance", control.command());
            assertTrue(control.ok());
        }
    }

    @Test
    void closingEndsSubscriptions() throws Exception {
        RiftClient client = rift.client(RiftProcess.PAPER_TOKEN).connect();
        Subscription subscription = client.subscribe("game.closing");
        assertTrue(client.isConnected());
        client.close();
        assertFalse(client.isConnected());
        assertTrue(client.closed().isDone());
        assertTrue(subscription.closed().isDone());
        assertThrows(RiftException.class, subscription::next);
        assertThrows(RiftException.class, () -> client.publish("game.closing", "x"));
    }

    private static java.security.cert.X509Certificate otherCertificate() throws Exception {
        Path directory = java.nio.file.Files.createTempDirectory("rift-java-other");
        Path keystore = directory.resolve("other.p12");
        Path pem = directory.resolve("other.pem");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        for (String[] command : new String[][] {
                {keytool, "-genkeypair", "-alias", "other", "-keyalg", "EC", "-groupname", "secp256r1",
                        "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-storetype", "PKCS12",
                        "-keystore", keystore.toString(), "-storepass", "changeit", "-keypass", "changeit"},
                {keytool, "-exportcert", "-rfc", "-alias", "other", "-keystore", keystore.toString(),
                        "-storepass", "changeit", "-file", pem.toString()}}) {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            assertEquals(0, process.waitFor());
        }
        return com.jotrorox.rift.internal.TrustManagers.readCertificates(pem).get(0);
    }
}
