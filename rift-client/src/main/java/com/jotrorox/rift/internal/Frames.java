package com.jotrorox.rift.internal;

import com.jotrorox.rift.RiftException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Reads and writes length-prefixed frames on a stream. */
public final class Frames {
    private Frames() {
    }

    public static Frame read(InputStream in) throws IOException {
        return read(in, Protocol.MAX_FRAME_BYTES);
    }

    public static Frame read(InputStream in, int maxBodyBytes) throws IOException {
        byte[] header = in.readNBytes(4);
        if (header.length < 4) {
            throw new EOFException("stream closed");
        }
        long size = ((header[0] & 0xFFL) << 24) | ((header[1] & 0xFFL) << 16)
                | ((header[2] & 0xFFL) << 8) | (header[3] & 0xFFL);
        if (size == 0 || size > maxBodyBytes) {
            throw new RiftException("frame length exceeds limit or is zero");
        }
        byte[] body = in.readNBytes((int) size);
        if (body.length < size) {
            throw new EOFException("stream closed inside a frame");
        }
        return new Frame(body);
    }

    /** Writes one frame whose body is {@code header} followed by {@code payload}, then flushes. */
    public static void write(OutputStream out, byte[] header, byte[] payload) throws IOException {
        long size = (long) header.length + payload.length;
        if (size == 0 || size > Protocol.MAX_FRAME_BYTES) {
            throw new RiftException("frame exceeds limit");
        }
        byte[] frame = new byte[4 + (int) size];
        frame[0] = (byte) (size >>> 24);
        frame[1] = (byte) (size >>> 16);
        frame[2] = (byte) (size >>> 8);
        frame[3] = (byte) size;
        System.arraycopy(header, 0, frame, 4, header.length);
        System.arraycopy(payload, 0, frame, 4 + header.length, payload.length);
        out.write(frame);
        out.flush();
    }

    public static void write(OutputStream out, byte[] body) throws IOException {
        write(out, body, new byte[0]);
    }

    public static void writeSimple(OutputStream out, int opcode) throws IOException {
        write(out, new byte[] {(byte) opcode});
    }
}
