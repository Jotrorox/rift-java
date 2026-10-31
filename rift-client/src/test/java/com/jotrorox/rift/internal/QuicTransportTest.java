package com.jotrorox.rift.internal;

import org.junit.jupiter.api.Test;

import java.net.ConnectException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuicTransportTest {
    @Test
    void retriesOnlyTheServerHelloRace() {
        assertTrue(QuicTransport.isHandshakeRace(new ConnectException("Handshake error: ")));
        assertTrue(QuicTransport.isHandshakeRace(new ConnectException(
                "Handshake error: tech.kwik.agent15.alert.UnexpectedMessageAlert: unexpected server hello message")));

        assertFalse(QuicTransport.isHandshakeRace(new ConnectException(
                "Handshake error: tech.kwik.agent15.alert.BadCertificateAlert: signature check failed")));
        assertFalse(QuicTransport.isHandshakeRace(new ConnectException(
                "Handshake error: Server closed connection: CONNECTION_REFUSED")));
        assertFalse(QuicTransport.isHandshakeRace(new ConnectException("Connection timed out after 10000 ms")));
        assertFalse(QuicTransport.isHandshakeRace(new ConnectException()));
    }
}
