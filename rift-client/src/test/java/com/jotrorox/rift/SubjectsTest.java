package com.jotrorox.rift;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubjectsTest {
    @ParameterizedTest
    @ValueSource(strings = {"game", "game.player.join", "game.*", "game.>", "*.player.*", ">", "_INBOX.paper.1"})
    void acceptsValidPatterns(String pattern) {
        assertTrue(Subjects.isValidPattern(pattern));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".", "game.", ".game", "game..join", "game.>.join", "game.pl*", "ga>me",
            "game player", "game.\tjoin", "game. "})
    void rejectsInvalidPatterns(String pattern) {
        assertFalse(Subjects.isValidPattern(pattern));
    }

    @Test
    void rejectsSubjectsOverTheByteLimit() {
        assertTrue(Subjects.isValidPattern("a".repeat(1024)));
        assertFalse(Subjects.isValidPattern("a".repeat(1025)));
        // The limit counts UTF-8 bytes, not characters.
        assertFalse(Subjects.isValidPattern("ä".repeat(513)));
    }

    @Test
    void literalsHaveNoWildcards() {
        assertTrue(Subjects.isValidLiteral("game.player.join"));
        assertFalse(Subjects.isValidLiteral("game.*"));
        assertFalse(Subjects.isValidLiteral("game.>"));
        assertThrows(IllegalArgumentException.class, () -> Subjects.requireLiteral("game.*"));
        assertEquals("game.chat", Subjects.requireLiteral("game.chat"));
    }

    @Test
    void matchesLikeTheBroker() {
        assertTrue(Subjects.matches("game.player.join", "game.player.join"));
        assertTrue(Subjects.matches("game.*.join", "game.player.join"));
        assertTrue(Subjects.matches("game.>", "game.player"));
        assertTrue(Subjects.matches("game.>", "game.player.join"));
        assertTrue(Subjects.matches(">", "game"));
        assertFalse(Subjects.matches("game.>", "game"));
        assertFalse(Subjects.matches("game.*", "game.player.join"));
        assertFalse(Subjects.matches("game.*", "game"));
        assertFalse(Subjects.matches("game.player", "game.player.join"));
        assertFalse(Subjects.matches("game.player.join", "game.player"));
        assertThrows(IllegalArgumentException.class, () -> Subjects.matches("game.*", "game.*"));
    }
}
