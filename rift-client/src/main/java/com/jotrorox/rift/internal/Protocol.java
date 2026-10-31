package com.jotrorox.rift.internal;

/** Constants of the Rift messaging wire protocol, version 1. */
public final class Protocol {
    public static final String ALPN = "rift-messaging/1";

    public static final int MAX_PAYLOAD_BYTES = 1024 * 1024;
    public static final int MAX_FRAME_BYTES = MAX_PAYLOAD_BYTES + 64 * 1024;
    public static final int MAX_STRING_BYTES = 4096;
    public static final int MAX_SUBJECT_BYTES = 1024;
    public static final int MAX_AUTH_FRAME_BYTES = MAX_STRING_BYTES + 3;
    public static final int MAX_FETCH = 256;
    public static final int MAX_REQUEST_TIMEOUT_MILLIS = 120_000;
    public static final int MAX_ACK_WAIT_MILLIS = 3_600_000;
    public static final int MAX_ACK_PENDING = 65_536;

    public static final int AUTH = 1;
    public static final int OK = 2;
    public static final int ERROR = 3;
    public static final int PUBLISH = 4;
    public static final int PUBLISH_ACK = 5;
    public static final int SUBSCRIBE = 6;
    public static final int MESSAGE = 7;
    public static final int PING = 8;
    public static final int REQUEST = 9;
    public static final int STREAM_PUBLISH = 10;
    public static final int STORED = 11;
    public static final int CONSUMER_OPEN = 12;
    public static final int CONSUMER_FETCH = 13;
    public static final int DELIVERY = 14;
    public static final int ACK = 15;

    public static final int ERROR_PERMISSION = 1;
    public static final int ERROR_OPERATION = 2;

    private Protocol() {
    }
}
