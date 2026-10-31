package com.jotrorox.rift.internal;

import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftException;
import com.jotrorox.rift.RiftPermissionException;
import com.jotrorox.rift.RiftRejectedException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FramesTest {
    @Test
    void encodesPublishExactlyAsSpecified() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] header = new FrameBuilder(Protocol.PUBLISH).string("a.b").string(null).toByteArray();
        Frames.write(out, header, new byte[] {9, 8});
        // u32 length, opcode, u16 length + "a.b", u16 0 for the absent reply, payload.
        assertArrayEquals(new byte[] {0, 0, 0, 10, 4, 0, 3, 'a', '.', 'b', 0, 0, 9, 8}, out.toByteArray());
    }

    @Test
    void encodesIntegersBigEndian() {
        byte[] body = new FrameBuilder(Protocol.STORED).u64(0x0102030405060708L).u32(0xFFFF_FFFFL).u16(0xABCD)
                .toByteArray();
        assertArrayEquals(new byte[] {11, 1, 2, 3, 4, 5, 6, 7, 8, -1, -1, -1, -1, (byte) 0xAB, (byte) 0xCD}, body);
        assertThrows(IllegalArgumentException.class, () -> new FrameBuilder(1).u32(-1));
        assertThrows(IllegalArgumentException.class, () -> new FrameBuilder(1).u32(1L << 32));
    }

    @Test
    void roundTripsMessages() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] header = new FrameBuilder(Protocol.MESSAGE).string("game.événement").string("_INBOX.x")
                .toByteArray();
        Frames.write(out, header, "payload".getBytes(StandardCharsets.UTF_8));
        Message message = Frames.read(new ByteArrayInputStream(out.toByteArray())).expect(Protocol.MESSAGE).message();
        assertEquals("game.événement", message.subject());
        assertEquals("_INBOX.x", message.reply());
        assertEquals("payload", message.payloadAsString());
    }

    @Test
    void emptyReplyBecomesNull() throws IOException {
        Frame frame = new Frame(new FrameBuilder(Protocol.MESSAGE).string("a").string("").toByteArray());
        assertNull(frame.expect(Protocol.MESSAGE).message().reply());
    }

    @Test
    void translatesErrorFrames() {
        Frame denied = new Frame(new FrameBuilder(Protocol.ERROR).u16(1).string("publication not permitted")
                .toByteArray());
        RiftPermissionException permission = assertThrows(RiftPermissionException.class, denied::expectOk);
        assertEquals("publication not permitted", permission.getMessage());

        Frame rejected = new Frame(new FrameBuilder(Protocol.ERROR).u16(2).string("unknown stream").toByteArray());
        assertEquals("unknown stream", assertThrows(RiftRejectedException.class, rejected::expectOk).getMessage());
    }

    @Test
    void rejectsMalformedFrames() {
        assertThrows(RiftException.class, () -> new Frame(new byte[] {Protocol.OK, 0}).expectOk());
        assertThrows(RiftException.class, () -> new Frame(new byte[] {Protocol.STORED, 0, 0}).expect(11).u64());
        assertThrows(RiftException.class, () -> new Frame(new byte[] {Protocol.MESSAGE}).expect(Protocol.OK));
        assertThrows(RiftException.class, () -> new Frame(new byte[] {7, 0, 2, (byte) 0xC3, 0x28}).expect(7).string());
        assertThrows(RiftException.class, () -> new Frame(new byte[] {7, 0, 1, '*', 0, 0}).expect(7).message());
    }

    @Test
    void enforcesLengthLimits() {
        assertThrows(RiftException.class, () -> Frames.read(new ByteArrayInputStream(new byte[] {0, 0, 0, 0})));
        assertThrows(RiftException.class,
                () -> Frames.read(new ByteArrayInputStream(new byte[] {0, 0x12, 0, 0, 1})));
        assertThrows(RiftException.class,
                () -> Frames.read(new ByteArrayInputStream(new byte[] {0, 0, 0x10, 4, 1}), Protocol.MAX_AUTH_FRAME_BYTES));
        assertThrows(IllegalArgumentException.class, () -> new FrameBuilder(1).string("x".repeat(4097)));
        assertThrows(RiftException.class,
                () -> Frames.write(new ByteArrayOutputStream(), new byte[1], new byte[Protocol.MAX_FRAME_BYTES]));
    }

    @Test
    void reportsTruncatedStreams() {
        assertThrows(EOFException.class, () -> Frames.read(new ByteArrayInputStream(new byte[] {0, 0})));
        assertThrows(EOFException.class, () -> Frames.read(new ByteArrayInputStream(new byte[] {0, 0, 0, 3, 2})));
    }
}
