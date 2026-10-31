package com.jotrorox.rift.internal;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/** Builds the fixed part of a frame body: the opcode followed by typed fields. */
public final class FrameBuilder {
    private final ByteArrayOutputStream body = new ByteArrayOutputStream(64);

    public FrameBuilder(int opcode) {
        body.write(opcode);
    }

    public FrameBuilder u16(int value) {
        body.write(value >>> 8);
        body.write(value);
        return this;
    }

    public FrameBuilder u32(long value) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException("value does not fit in u32: " + value);
        }
        for (int shift = 24; shift >= 0; shift -= 8) {
            body.write((int) (value >>> shift));
        }
        return this;
    }

    public FrameBuilder u64(long value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            body.write((int) (value >>> shift));
        }
        return this;
    }

    /** Writes a {@code u16} length followed by the UTF-8 bytes; {@code null} encodes as empty. */
    public FrameBuilder string(String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Protocol.MAX_STRING_BYTES) {
            throw new IllegalArgumentException("string exceeds the protocol limit of "
                    + Protocol.MAX_STRING_BYTES + " bytes");
        }
        u16(bytes.length);
        body.writeBytes(bytes);
        return this;
    }

    public byte[] toByteArray() {
        return body.toByteArray();
    }
}
