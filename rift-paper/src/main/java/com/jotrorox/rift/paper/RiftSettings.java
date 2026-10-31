package com.jotrorox.rift.paper;

import com.jotrorox.rift.RiftClient;
import org.bukkit.configuration.ConfigurationSection;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Connection settings for a Paper server, usually read from the plugin's
 * {@code config.yml}:
 *
 * <pre>{@code
 * rift:
 *   host: 127.0.0.1
 *   port: 4222
 *   server-name: localhost        # DNS name in the certificate; defaults to host
 *   certificate: rift-cert.pem    # relative to the plugin's data folder
 *   token-env: RIFT_PAPER_TOKEN   # or token: "..." (prefer the environment)
 *   operation-timeout-seconds: 30
 * }</pre>
 *
 * @param host                 host of Rift's messaging listener
 * @param port                 UDP port of Rift's messaging listener
 * @param serverName           DNS name verified in the server certificate
 * @param certificate          PEM file with the trusted CA or server certificate
 * @param token                the principal's token
 * @param operationTimeout     deadline for individual operations
 */
public record RiftSettings(String host, int port, String serverName, Path certificate, String token,
                           Duration operationTimeout) {
    public static final int DEFAULT_PORT = 4222;
    public static final String DEFAULT_TOKEN_ENV = "RIFT_PAPER_TOKEN";

    public RiftSettings {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(certificate, "certificate");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be 1..65535");
        }
        if (serverName == null || serverName.isBlank()) {
            serverName = host;
        }
    }

    /** Reads settings from a configuration section such as {@code rift} in {@code config.yml}. */
    public static RiftSettings from(ConfigurationSection section, Path dataFolder) {
        return from(section.getValues(false), dataFolder, System::getenv);
    }

    /**
     * Reads settings from configuration values.
     *
     * @param environment looks up environment variables, normally {@code System::getenv}
     * @throws IllegalArgumentException if required values are missing or invalid
     */
    public static RiftSettings from(Map<String, ?> values, Path dataFolder, UnaryOperator<String> environment) {
        String host = text(values, "host", "127.0.0.1");
        int port = number(values, "port", DEFAULT_PORT);
        String serverName = text(values, "server-name", null);
        String certificate = text(values, "certificate", null);
        if (certificate == null) {
            throw new IllegalArgumentException("rift.certificate must name the PEM file with Rift's certificate");
        }
        String token = text(values, "token", null);
        if (token == null) {
            String variable = text(values, "token-env", DEFAULT_TOKEN_ENV);
            token = environment.apply(variable);
            if (token == null || token.isEmpty()) {
                throw new IllegalArgumentException("set the environment variable " + variable
                        + " to the token Rift expects for this server");
            }
        }
        Duration timeout = Duration.ofSeconds(number(values, "operation-timeout-seconds",
                (int) RiftClient.DEFAULT_OPERATION_TIMEOUT.toSeconds()));
        return new RiftSettings(host, port, serverName, dataFolder.resolve(certificate), token, timeout);
    }

    /** A client builder configured with these settings. */
    public RiftClient.Builder clientBuilder() throws IOException {
        return RiftClient.builder()
                .address(host, port)
                .serverName(serverName)
                .trustCertificates(certificate)
                .token(token)
                .operationTimeout(operationTimeout);
    }

    @Override
    public String toString() {
        // Never print the token.
        return "RiftSettings[host=" + host + ", port=" + port + ", serverName=" + serverName
                + ", certificate=" + certificate + "]";
    }

    private static String text(Map<String, ?> values, String key, String fallback) {
        Object value = values.get(key);
        if (value == null) {
            return fallback;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? fallback : text;
    }

    private static int number(Map<String, ?> values, String key, int fallback) {
        Object value = values.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("rift." + key + " must be a number", e);
        }
    }
}
