package com.jotrorox.rift.internal;

import com.jotrorox.rift.Message;
import com.jotrorox.rift.RiftException;
import com.jotrorox.rift.RiftPermissionException;
import com.jotrorox.rift.RiftRejectedException;
import com.jotrorox.rift.Subjects;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A received frame body with a read cursor. All integers are unsigned big endian. */
public final class Frame {
    private final ByteBuffer body;

    public Frame(byte[] body) {
        this.body = ByteBuffer.wrap(body);
    }

    public int opcode() {
        return body.get(0) & 0xFF;
    }

    public int u8() throws RiftException {
        require(1);
        return body.get() & 0xFF;
    }

    public int u16() throws RiftException {
        require(2);
        return body.getShort() & 0xFFFF;
    }

    public long u32() throws RiftException {
        require(4);
        return body.getInt() & 0xFFFF_FFFFL;
    }

    public long u64() throws RiftException {
        require(8);
        return body.getLong();
    }

    public String string() throws RiftException {
        int length = u16();
        if (length > Protocol.MAX_STRING_BYTES) {
            throw new RiftException("invalid string length");
        }
        require(length);
        ByteBuffer slice = body.slice(body.position(), length);
        body.position(body.position() + length);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(slice)
                    .toString();
        } catch (CharacterCodingException e) {
            throw new RiftException("string must be UTF-8", e);
        }
    }

    /** All remaining bytes of the frame. */
    public byte[] rest() {
        byte[] rest = Arrays.copyOfRange(body.array(), body.position(), body.limit());
        body.position(body.limit());
        return rest;
    }

    public void end() throws RiftException {
        if (body.hasRemaining()) {
            throw new RiftException("unexpected trailing frame bytes");
        }
    }

    /**
     * Consumes the opcode, translating ERROR frames into exceptions.
     *
     * @throws RiftPermissionException for permission errors reported by the server
     * @throws RiftRejectedException   for other errors reported by the server
     * @throws RiftException           if the frame has another unexpected opcode
     */
    public Frame expect(int opcode) throws RiftException {
        int actual = u8();
        if (actual == Protocol.ERROR) {
            int code = u16();
            String message = string();
            if (code == Protocol.ERROR_PERMISSION) {
                throw new RiftPermissionException(message);
            }
            throw new RiftRejectedException(message);
        }
        if (actual != opcode) {
            throw new RiftException("unexpected response opcode " + actual + ", expected " + opcode);
        }
        return this;
    }

    public void expectOk() throws RiftException {
        expect(Protocol.OK).end();
    }

    /** Reads the {@code subject:str, reply:str, payload} layout shared by MESSAGE and DELIVERY. */
    public Message message() throws RiftException {
        String subject = string();
        String reply = string();
        if (!Subjects.isValidLiteral(subject) || (!reply.isEmpty() && !Subjects.isValidLiteral(reply))) {
            throw new RiftException("server sent an invalid subject");
        }
        byte[] payload = rest();
        if (payload.length > Protocol.MAX_PAYLOAD_BYTES) {
            throw new RiftException("payload exceeds 1 MiB transport limit");
        }
        return new Message(subject, reply, payload);
    }

    private void require(int bytes) throws RiftException {
        if (body.remaining() < bytes) {
            throw new RiftException("truncated frame");
        }
    }
}
