package com.airbnb.plog.commands

import com.airbnb.plog.Utils
import com.airbnb.plog.stats.SimpleStatisticsReporter
import com.airbnb.plog.utils.ByteBufs
import com.typesafe.config.ConfigFactory
import io.netty.channel.embedded.EmbeddedChannel

import java.security.Permission
import java.util.regex.Pattern

class FourLetterCommandHandlerTest extends GroovyTestCase {
    static private final config = ConfigFactory.load()
    static private final stats = new SimpleStatisticsReporter(null)

    void testCaseSensitivity() {
        expectAnswer(new FourLetterCommand('PinG', Utils.clientAddr, ''.bytes), ~/PONG/)
    }

    void testNakedPing() {
        expectAnswer(new FourLetterCommand(FourLetterCommand.PING, Utils.clientAddr, ''.bytes), ~/PONG/)
    }

    void testPingPayload() {
        expectAnswer(new FourLetterCommand(FourLetterCommand.PING, Utils.clientAddr, 'foo'.bytes), ~/PONGfoo/)
    }

    void testEnvi() {
        expectAnswer(new FourLetterCommand(FourLetterCommand.ENVI, Utils.clientAddr, null), ~/Config\(.*/)
    }

    void testStat() {
        final statsJSON = stats.toJSON()
        final laxUptimeAndCommandCount = statsJSON
                .replaceAll('\\{', '\\\\{').replaceAll('\\}', '\\\\}')
                .replaceAll('\\[', '\\\\[').replaceAll('\\]', '\\\\]')
                .replaceFirst('[0-9]+', '[0-9]+') // uptime
                .replaceFirst('"v0_commands":[0-9]+', '"v0_commands":[0-9]+')
        final pattern = Pattern.compile(laxUptimeAndCommandCount)
        expectAnswer(new FourLetterCommand(FourLetterCommand.STAT, Utils.clientAddr, null), pattern)
    }

    static final private class ExitFailure extends Exception {}

    void testKill() {
        System.setSecurityManager(new SecurityManager() {
            @Override
            void checkExit(int status) {
                super.checkExit(status)
                throw new ExitFailure();
            }

            @Override
            void checkPermission(Permission perm, Object context) {
            }

            @Override
            void checkPermission(Permission perm) {
            }
        })

        shouldFail ExitFailure, {
            runTest { EmbeddedChannel channel ->
                channel.writeInbound(new FourLetterCommand(
                        FourLetterCommand.KILL,
                        Utils.clientAddr,
                        null))
            }
        }
        System.setSecurityManager(null)
    }

    void testUnknownIsTracked() {
        runTest { EmbeddedChannel channel ->
            final before = stats.receivedUnknownCommand()
            assert !channel.writeInbound(new FourLetterCommand(
                    'burp',
                    Utils.clientAddr,
                    null))
            assert stats.receivedUnknownCommand() == before + 2
        }
    }

    private void expectAnswer(FourLetterCommand req, Pattern expectedReply) {
        runTest { EmbeddedChannel channel ->
            final commandsBefore = stats.receivedV0Command()
            assert !channel.writeInbound(req)
            final reply = (io.netty.channel.socket.DatagramPacket) channel.readOutbound()
            final payload = new String(ByteBufs.toByteArray(reply.content()))
            assert payload ==~ expectedReply
            assert reply.recipient() == Utils.clientAddr
            final commandsAfter = stats.receivedV0Command()
            assert commandsAfter == commandsBefore + 2
        }
    }

    private void runTest(Closure test) {
        final channel = new EmbeddedChannel(new FourLetterCommandHandler(stats, config))
        test.call(channel)
        assert !channel.finish()
    }
}
