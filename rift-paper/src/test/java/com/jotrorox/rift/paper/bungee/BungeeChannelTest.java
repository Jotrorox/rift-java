package com.jotrorox.rift.paper.bungee;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BungeeChannelTest {
    @Test
    void carriesOnlyOverConnectionsListeningOnTheChannel() {
        assertTrue(BungeeChannel.canCarry(Set.of("minecraft:brand", "bungeecord:main")));
        assertTrue(BungeeChannel.canCarry(Set.of("BungeeCord")));
        // Players still joining have not had the channel registered yet.
        assertFalse(BungeeChannel.canCarry(Set.of()));
        assertFalse(BungeeChannel.canCarry(Set.of("minecraft:brand", "velocity:player_info")));
    }
}
