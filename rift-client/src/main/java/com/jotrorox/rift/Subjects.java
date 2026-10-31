package com.jotrorox.rift;

import com.jotrorox.rift.internal.Protocol;

import java.nio.charset.StandardCharsets;

/**
 * Subject syntax shared by publishers and subscribers.
 *
 * <p>Subjects are dot-separated tokens such as {@code game.player.join}. In
 * subscription patterns {@code *} matches exactly one token and a terminal
 * {@code >} matches one or more remaining tokens. Publications always use
 * literal subjects. Rift validates subjects itself; these checks let callers
 * fail fast before a network round trip.
 */
public final class Subjects {
    private Subjects() {
    }

    /** Whether {@code pattern} is a valid subscription pattern. */
    public static boolean isValidPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return false;
        }
        if (pattern.getBytes(StandardCharsets.UTF_8).length > Protocol.MAX_SUBJECT_BYTES) {
            return false;
        }
        String[] tokens = pattern.split("\\.", -1);
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.isEmpty() || hasForbiddenCharacter(token)) {
                return false;
            }
            boolean wildcard = token.indexOf('*') >= 0 || token.indexOf('>') >= 0;
            if (wildcard && !token.equals("*") && !token.equals(">")) {
                return false;
            }
            if (token.equals(">") && i != tokens.length - 1) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code subject} is a valid literal subject usable for publication. */
    public static boolean isValidLiteral(String subject) {
        return isValidPattern(subject) && subject.indexOf('*') < 0 && subject.indexOf('>') < 0;
    }

    /**
     * Whether the literal {@code subject} is matched by {@code pattern}.
     *
     * @throws IllegalArgumentException if either argument is invalid
     */
    public static boolean matches(String pattern, String subject) {
        requirePattern(pattern);
        requireLiteral(subject);
        String[] p = pattern.split("\\.", -1);
        String[] s = subject.split("\\.", -1);
        for (int i = 0; i < p.length; i++) {
            if (p[i].equals(">")) {
                return s.length > i;
            }
            if (i >= s.length || (!p[i].equals("*") && !p[i].equals(s[i]))) {
                return false;
            }
        }
        return p.length == s.length;
    }

    /**
     * Returns {@code pattern} unchanged if it is a valid subscription pattern.
     *
     * @throws IllegalArgumentException if the pattern is invalid
     */
    public static String requirePattern(String pattern) {
        if (!isValidPattern(pattern)) {
            throw new IllegalArgumentException("invalid subject pattern: " + pattern);
        }
        return pattern;
    }

    /**
     * Returns {@code subject} unchanged if it is a valid literal subject.
     *
     * @throws IllegalArgumentException if the subject is invalid or contains wildcards
     */
    public static String requireLiteral(String subject) {
        if (!isValidLiteral(subject)) {
            throw new IllegalArgumentException("invalid literal subject: " + subject);
        }
        return subject;
    }

    private static boolean hasForbiddenCharacter(String token) {
        return token.codePoints().anyMatch(c ->
                Character.isWhitespace(c) || Character.isSpaceChar(c) || Character.isISOControl(c));
    }
}
