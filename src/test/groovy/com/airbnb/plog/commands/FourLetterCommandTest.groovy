package com.airbnb.plog.commands

import com.airbnb.plog.Utils

class FourLetterCommandTest extends GroovyTestCase {
    private final emptyTrail = ''.bytes
    private static final shortTrail = 'foo'.bytes

    void testCaseSensitivity() {
        for (cmdStr in ['ping', 'PING', 'PiNg']) {
            final cmd = new FourLetterCommand(cmdStr, Utils.clientAddr, emptyTrail)
            assert cmd.is(FourLetterCommand.PING)
        }
    }

    void testTrailIsIgnored() {
        assert new FourLetterCommand('stat', Utils.clientAddr, 'foo'.bytes).is('STAT')
    }

    void testTailPreserved() {
        assert new FourLetterCommand('PING', Utils.clientAddr, shortTrail).trail == shortTrail
        assert new FourLetterCommand('PING', Utils.clientAddr, emptyTrail).trail == emptyTrail
    }

    void testToString() {
        final repr = new FourLetterCommand('ping', Utils.clientAddr, 'foo'.bytes).toString()
        assert repr == 'FourLetterCommand(command=PING, sender=/192.0.2.0:9, trail=[102, 111, 111])'
    }
}
