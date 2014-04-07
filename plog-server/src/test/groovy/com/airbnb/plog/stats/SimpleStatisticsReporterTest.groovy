package com.airbnb.plog.stats

import com.airbnb.plog.fragmentation.Defragmenter
import com.typesafe.config.ConfigFactory
import groovy.json.JsonSlurper

class SimpleStatisticsReporterTest extends GroovyTestCase {
    private final static slurper = new JsonSlurper()

    private final static defragConfig =
            ConfigFactory.load().getConfig('plog.udp.defaults.defrag')

    void testMinimalIsValidJSON() {
        testValidJSON(new SimpleStatisticsReporter())
    }

    void testIsValidJSONWithDefrag() {
        final stats = new SimpleStatisticsReporter()
        stats.withDefrag(new Defragmenter(stats, defragConfig))
        testValidJSON(stats)
    }

    void testLogLogStatsShape() {
        final stats = new SimpleStatisticsReporter()
        final droppedStats = slurper.parseText(stats.toJSON())['dropped_fragments']
        for (i in 0..16)
            assert droppedStats[i].size == i + 1
    }

    void testLogLogStatsCorrectlyRendered() {
        final stats = new SimpleStatisticsReporter()

        stats.missingFragmentInDroppedMessage(0, 1)
        2.times { stats.missingFragmentInDroppedMessage(1, 2) }
        3.times { stats.missingFragmentInDroppedMessage(2, 3) }
        4.times { stats.missingFragmentInDroppedMessage(2, 4) }
        5.times { stats.missingFragmentInDroppedMessage(0, 8) }
        6.times { stats.missingFragmentInDroppedMessage(6, 10) }

        final droppedStats = slurper.parseText(stats.toJSON())['dropped_fragments']

        assert droppedStats[0][0] == 1
        assert droppedStats[1][1] == 2
        assert droppedStats[2][2] == 7
        assert droppedStats[3][0] == 5
        assert droppedStats[4][3] == 6
    }

    private void testValidJSON(SimpleStatisticsReporter stats) {
        final parsed = slurper.parseText(stats.toJSON())
        assert parsed instanceof Map
    }

    void testSimpleCounters() {
        final stats = new SimpleStatisticsReporter()
        final methods = [
                'receivedUdpSimpleMessage',
                'receivedUdpInvalidVersion',
                'receivedV0InvalidType',
                'receivedV0Command',
                'receivedUnknownCommand',
                'receivedV0MultipartMessage',
                'receivedV0InvalidMultipartHeader',
                'failedToSend',
                'exception'
        ]
        for (method in methods)
            assert stats."$method"() == 1
        // let's make sure we didn't increment more than we wanted...
        for (method in methods)
            assert stats."$method"() == 2
    }

    void testJumpingCounters() {
        final stats = new SimpleStatisticsReporter()
        for (method in ['foundHolesFromDeadPort', 'foundHolesFromNewMessage']) {
            assert stats."$method"(0) == 0
            assert stats."$method"(1) == 1
            assert stats."$method"(5) == 6
            assert stats."$method"(0) == 6
        }
    }

    void testReceivedV0MultipartFragment() {
        final stats = new SimpleStatisticsReporter()
        for (exp in 0..<Short.SIZE)
            assert stats.receivedV0MultipartFragment(2**exp) == 1
        for (exp in 1..<Short.SIZE) {
            assert stats.receivedV0MultipartFragment(2**exp + 1) == 2
            assert stats.receivedV0MultipartFragment(2**(exp + 1) - 1) == 3
        }
    }

    void testReceivedV0InvalidChecksum() {
        final stats = new SimpleStatisticsReporter()
        for (exp in 0..<Short.SIZE)
            assert stats.receivedV0InvalidChecksum(2**exp + 1) == 1
        for (exp in 1..<Short.SIZE) {
            assert stats.receivedV0InvalidChecksum(2**exp + 2) == 2
            assert stats.receivedV0InvalidChecksum(2**(exp + 1)) == 3
        }
    }

    void testCantProvideTwoDefragmenters() {
        final stats = new SimpleStatisticsReporter()
        stats.withDefrag(new Defragmenter(stats, defragConfig))
    }

    void testLogLogCounters() {
        final stats = new SimpleStatisticsReporter()
        for (method in ['receivedV0InvalidMultipartFragment', 'missingFragmentInDroppedMessage']) {
            for (fragIndexExp in 0..<Short.SIZE) {
                for (fragmentCountExp in 0..<Short.SIZE)
                    assert stats."$method"(
                            2**fragIndexExp,
                            2**fragmentCountExp + 1) == 1
                if (fragIndexExp == 0)
                    continue
                for (fragmentCountExp in 1..<Short.SIZE) {
                    assert stats."$method"(
                            2**fragIndexExp + 1,
                            2**fragmentCountExp + 2) == 2
                    assert stats."$method"(
                            (2**(fragIndexExp + 1) - 1),
                            2**(fragmentCountExp + 1)) == 3
                }
            }
        }
    }
}
