package com.airbnb.plog.commands

class FourLetterCommandTest extends GroovyTestCase {
    private static final addr = new InetSocketAddress(0)
    private static final emptyTrail = ''.bytes
    private static final shortTrail = 'foo'.bytes

    void testCaseSensitivity() {
        for (cmdStr in ['ping', 'PING', 'PiNg']) {
            final cmd = new FourLetterCommand(cmdStr, addr, emptyTrail)
            assert cmd.is(FourLetterCommand.PING)
        }
    }

    void testTrailIsIgnored() {
        assert new FourLetterCommand('stat', addr, 'foo'.bytes).is('STAT')
    }

    void testTailPreserved() {
        assert new FourLetterCommand('PING', addr, shortTrail).trail == shortTrail
        assert new FourLetterCommand('PING', addr, emptyTrail).trail == emptyTrail
    }
}
