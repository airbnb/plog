package com.airbnb.plog.packetloss

class PortHoleDetectorTest extends GroovyTestCase {
    static def IDS_WITHOUT_FALSE_NEGATIVES = [
            Integer.MIN_VALUE + 100,
            -1000, 0, 1000,
            Integer.MAX_VALUE - 100,
    ]
    static def INTERESTING_IDS = [
            Integer.MIN_VALUE,
            Integer.MIN_VALUE + 5,
            Integer.MAX_VALUE - 5,
            Integer.MAX_VALUE
    ] + IDS_WITHOUT_FALSE_NEGATIVES

    void testInsufficientCapacity() {
        shouldFail { new PortHoleDetector(0) }
        new PortHoleDetector(1)
    }

    void testMaxHoleTooLow() {
        final detector = new PortHoleDetector(1)
        shouldFail PortHoleDetector.MaxHoleTooSmall, { detector.countTotalHoles(-1) }
        shouldFail PortHoleDetector.MaxHoleTooSmall, { detector.countTotalHoles(0) }
        shouldFail PortHoleDetector.MaxHoleTooSmall, { detector.ensurePresent(0, -1) }
        shouldFail PortHoleDetector.MaxHoleTooSmall, { detector.ensurePresent(0, 0) }
        assert detector.ensurePresent(0, 1) == 0
        assert detector.countTotalHoles(1) == 0
    }

    void testKeepsSortedInternally() {
        final ordered = 1..4
        final size = ordered.size()
        final marginToAvoidResets = size - 1
        for (seq in ordered.combinations()) {
            final detector = new PortHoleDetector(size)
            for (entry in seq)
                detector.ensurePresent(entry, marginToAvoidResets)
            assert detector.entries == ordered
        }
    }

    void testNoHolesWhenOrdered() {
        for (baseId in INTERESTING_IDS) {
            final detector = new PortHoleDetector(1)
            for (id in baseId..(baseId + 5)) {
                assert detector.ensurePresent(id, 1) == 0
                assert detector.entries == [id]
            }
            assert detector.countTotalHoles(1) == 0
        }
    }

    void testFindHolesInTheEnd() {
        final numberOfEntries = 5
        for (holeSize in 1..5) {
            final baseValues = (0..<numberOfEntries).collect { (holeSize + 1) * it }
            for (baseId in IDS_WITHOUT_FALSE_NEGATIVES) {
                final values = baseValues.collect { baseId + it }
                for (seq in values.combinations()) {
                    final detector = new PortHoleDetector(numberOfEntries)
                    assert seq.collect { detector.ensurePresent(it, holeSize + 1) }.sum() == 0
                    assert detector.countTotalHoles(holeSize + 1) == (numberOfEntries - 1) * holeSize
                    if (holeSize > 2)
                        assert detector.countTotalHoles(holeSize - 1) == 0
                }
            }
        }
    }

    void testFindHolesInFlight() {
        final numberOfEntries = 5
        for (holeSize in 1..5) {
            final baseValues = (0..<numberOfEntries).collect { (holeSize + 1) * it }
            for (baseId in IDS_WITHOUT_FALSE_NEGATIVES) {
                final values = baseValues.collect { baseId + it }
                for (seq in values.combinations()) {
                    final detector = new PortHoleDetector(1)
                    final marginAgainstResets = holeSize * numberOfEntries
                    assert seq.collect {
                        detector.ensurePresent(it, marginAgainstResets)
                    }.sum() == (numberOfEntries - 1) * holeSize
                }
            }
        }
    }

    void testMinMaxTracked() {
        final detector = new PortHoleDetector(1)
        detector.ensurePresent(0, 1)
        assert detector.minSeen == 0
        assert detector.maxSeen == 0
        detector.ensurePresent(-1, 1)
        assert detector.minSeen == -1
        assert detector.maxSeen == 0
        detector.ensurePresent(1, 2)
        assert detector.minSeen == -1
        assert detector.maxSeen == 1
    }

    void testResetsWhenHigh() {
        final detector = new PortHoleDetector(2)
        detector.ensurePresent(0, 5)
        detector.ensurePresent(10, 5)
        assert detector.entries == [Integer.MIN_VALUE, 10]
    }

    void testResetsWhenLow() {
        final detector = new PortHoleDetector(2)
        detector.ensurePresent(0, 5)
        detector.ensurePresent(-10, 5)
        assert detector.entries == [Integer.MIN_VALUE, -10]
    }

    void testIgnoreBigHoles() {
        final detector = new PortHoleDetector(2)
        assert detector.ensurePresent(0, 5) == 0
        assert detector.ensurePresent(10, 5) == 0
        assert detector.countTotalHoles(5) == 0
    }
}
