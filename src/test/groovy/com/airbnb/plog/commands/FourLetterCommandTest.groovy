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
}
