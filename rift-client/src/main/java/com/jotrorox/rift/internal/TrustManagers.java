package com.jotrorox.rift.internal;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Builds trust managers that accept only explicitly configured certificates. */
public final class TrustManagers {
    private TrustManagers() {
    }

    /** Reads every certificate from a PEM (or DER) file. */
    public static List<X509Certificate> readCertificates(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            List<X509Certificate> certificates = new ArrayList<>();
            for (Certificate certificate : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                certificates.add((X509Certificate) certificate);
            }
            if (certificates.isEmpty()) {
                throw new IOException("certificate PEM (" + file + "): no certificates found");
            }
            return certificates;
        } catch (GeneralSecurityException e) {
            throw new IOException("certificate PEM (" + file + "): " + e.getMessage(), e);
        }
    }

    /** A PKIX trust manager whose only trust anchors are {@code certificates}. */
    public static X509TrustManager trusting(Collection<X509Certificate> certificates) throws IOException {
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("at least one trusted certificate is required");
        }
        try {
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int index = 0;
            for (X509Certificate certificate : certificates) {
                store.setCertificateEntry("rift-" + index++, certificate);
            }
            TrustManagerFactory factory = TrustManagerFactory.getInstance("PKIX");
            factory.init(store);
            for (TrustManager manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager x509) {
                    return x509;
                }
            }
            throw new IOException("no X.509 trust manager available");
        } catch (GeneralSecurityException e) {
            throw new IOException("could not build trust store: " + e.getMessage(), e);
        }
    }
}
