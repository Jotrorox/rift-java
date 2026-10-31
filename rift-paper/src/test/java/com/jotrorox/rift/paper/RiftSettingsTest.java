package com.jotrorox.rift.paper;

import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RiftSettingsTest {
    private static final Path DATA = Path.of("plugins", "Example");

    @Test
    void readsYamlWithTheTokenFromTheEnvironment() throws Exception {
        YamlConfiguration yaml = new YamlConfiguration();
        yaml.loadFromString("""
                rift:
                  host: rift.internal
                  port: 7443
                  server-name: messaging.example.com
                  certificate: certs/rift.pem
                  token-env: MY_TOKEN
                  operation-timeout-seconds: 10
                """);
        RiftSettings settings = RiftSettings.from(yaml.getConfigurationSection("rift").getValues(false), DATA,
                name -> name.equals("MY_TOKEN") ? "secret-token" : null);
        assertEquals("rift.internal", settings.host());
        assertEquals(7443, settings.port());
        assertEquals("messaging.example.com", settings.serverName());
        assertEquals(DATA.resolve("certs/rift.pem"), settings.certificate());
        assertEquals("secret-token", settings.token());
        assertEquals(Duration.ofSeconds(10), settings.operationTimeout());
    }

    @Test
    void appliesDefaults() {
        RiftSettings settings = RiftSettings.from(Map.of("certificate", "rift.pem"), DATA,
                name -> name.equals(RiftSettings.DEFAULT_TOKEN_ENV) ? "t" : null);
        assertEquals("127.0.0.1", settings.host());
        assertEquals(RiftSettings.DEFAULT_PORT, settings.port());
        assertEquals("127.0.0.1", settings.serverName());
        assertEquals(Duration.ofSeconds(30), settings.operationTimeout());
    }

    @Test
    void acceptsAnInlineToken() {
        RiftSettings settings = RiftSettings.from(Map.of("certificate", "rift.pem", "token", "inline", "port", "5000"),
                DATA, name -> null);
        assertEquals("inline", settings.token());
        assertEquals(5000, settings.port());
    }

    @Test
    void reportsMissingValues() {
        IllegalArgumentException certificate = assertThrows(IllegalArgumentException.class,
                () -> RiftSettings.from(Map.of("token", "t"), DATA, name -> null));
        assertTrue(certificate.getMessage().contains("certificate"));
        IllegalArgumentException token = assertThrows(IllegalArgumentException.class,
                () -> RiftSettings.from(Map.of("certificate", "c.pem", "token-env", "UNSET"), DATA, name -> null));
        assertTrue(token.getMessage().contains("UNSET"));
        assertThrows(IllegalArgumentException.class,
                () -> RiftSettings.from(Map.of("certificate", "c.pem", "token", "t", "port", "abc"), DATA, n -> null));
        assertThrows(IllegalArgumentException.class,
                () -> RiftSettings.from(Map.of("certificate", "c.pem", "token", "t", "port", 70000), DATA, n -> null));
    }

    @Test
    void neverPrintsTheToken() {
        RiftSettings settings = RiftSettings.from(Map.of("certificate", "c.pem", "token", "very-secret"), DATA,
                name -> null);
        assertFalse(settings.toString().contains("very-secret"));
    }
}
