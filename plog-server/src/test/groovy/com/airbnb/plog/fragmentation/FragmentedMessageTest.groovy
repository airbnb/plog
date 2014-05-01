package com.airbnb.plog.fragmentation

import com.airbnb.plog.stats.SimpleStatisticsReporter
import com.airbnb.plog.stats.StatisticsReporter
import com.airbnb.plog.utils.ByteBufs
import io.netty.buffer.Unpooled

class FragmentedMessageTest extends GroovyTestCase {
    private static final StatisticsReporter stats = new SimpleStatisticsReporter()

    // Always go with id=0 & hash=0
    private static create(int count, int index, int fsize, int length, byte[] payload) {
        FragmentedMessage.fromFragment(
                new Fragment(count, index, fsize, 0, length, 0, Unpooled.wrappedBuffer(payload)), stats)
    }

    private
    static ingest(FragmentedMessage msg, int count, int index, int fsize, int length, byte[] payload) {
        msg.ingestFragment(new Fragment(count, index, fsize, 0, length, 0, Unpooled.wrappedBuffer(payload)), stats)
    }

    private static
    final read(FragmentedMessage msg) {
        return ByteBufs.toByteArray(msg.payload)
    }

    void testSurvivesEmptiness() {
        final msg = create(1, 0, 0, 0, ''.bytes)
        assert msg.isComplete()
        assert msg.contentLength == 0
        assert read(msg) == []
    }

    void testProtectsIntermediateStateAndReconstitutesInOrder() {
        for (length in 2..5) {
            for (indices in (0..<length).combinations()) {
                final msg = create(length, indices[0], 1, length, indices[0].toString().bytes)
                assert !msg.isComplete()
                shouldFail(IllegalStateException, { msg.payload })
                if (length > 2)
                    for (i in indices[1..-2]) {
                        ingest(msg, length, i, 1, length, i.toString().bytes)
                        assert !msg.isComplete()
                        shouldFail(IllegalStateException, { msg.payload })
                    }
                ingest(msg, length, indices[-1], 1, length, indices[-1].toString().bytes)
                final content = read(msg)
                assert content.length == length && '01234'.startsWith(new String(content))
            }
        }
    }

    void testCatchesInvalidLengthForLast() {
        // last can come first, how convenient :)
        final long initial = stats.receivedV0InvalidMultipartFragment(4, 5)

        create(5, 4, 10, 45, '0123'.bytes) // too short
        assert stats.receivedV0InvalidMultipartFragment(4, 5) == initial + 2

        create(5, 4, 10, 45, '01234'.bytes) // correct
        assert stats.receivedV0InvalidMultipartFragment(4, 5) == initial + 3

        create(5, 4, 10, 45, '012345'.bytes) // too long
        assert stats.receivedV0InvalidMultipartFragment(4, 5) == initial + 5
    }

    void testCatchesInvalidLengthForMiddle() {
        // last can come first, how convenient :)
        final long initial = stats.receivedV0InvalidMultipartFragment(2, 5)

        create(5, 2, 10, 45, '012345678'.bytes) // too short
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 2

        create(5, 2, 10, 45, '0123456789'.bytes) // correct
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 3

        create(5, 2, 10, 45, '01234567890'.bytes) // too long
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 5
    }

    void testCatchesInvalidLengthForSecondArrived() {
        // last can come first, how convenient :)
        final long initial = stats.receivedV0InvalidMultipartFragment(2, 5)

        final getsTooLittle = create(5, 2, 10, 45, '0123456789'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 1
        ingest(getsTooLittle, 5, 2, 10, 45, '012345678'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 3

        final getsTooMuch = create(5, 2, 10, 45, '0123456789'.bytes) // too short
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 4
        ingest(getsTooMuch, 5, 2, 10, 45, '01234567890'.bytes) // too long
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 6
    }

    void testReconstitutesInOrder() {
        final msg = create(2, 1, 1, 2, [1] as byte[])
        ingest(msg, 2, 0, 1, 2, [2] as byte[])
        assert msg.isComplete()
        assert ByteBufs.toByteArray(msg.payload) == [2, 1]
    }

    void testCatchesFragmentSizeInconsistency() {
        final long initial = stats.receivedV0InvalidMultipartFragment(2, 5)
        final seesInconsistentSizes = create(5, 2, 10, 45, '0123456789'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 1
        ingest(seesInconsistentSizes, 5, 3, 12, 45, '0123456789'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 3
    }

    void testCatchesFragmentCountInconsistency() {
        final long initial = stats.receivedV0InvalidMultipartFragment(2, 5)
        final seesInconsistentSizes = create(5, 2, 10, 45, '0123456789'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 1
        ingest(seesInconsistentSizes, 4, 3, 10, 45, '0123456789'.bytes)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 3
    }

    void testCatchesChecksumInconsistency() {
        final long initial = stats.receivedV0InvalidMultipartFragment(2, 5)
        final msg = FragmentedMessage.fromFragment(
                new Fragment(5, 2, 10, 0, 45, 42, Unpooled.wrappedBuffer('0123456789'.bytes)), stats)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 1
        msg.ingestFragment(new Fragment(5, 2, 10, 0, 45, 24, Unpooled.wrappedBuffer('0123456789'.bytes)), stats)
        assert stats.receivedV0InvalidMultipartFragment(2, 5) == initial + 3
    }
}
