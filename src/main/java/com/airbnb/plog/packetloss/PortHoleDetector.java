package com.airbnb.plog.packetloss;

import java.util.Arrays;

public class PortHoleDetector {
    private final int[] entries;

    PortHoleDetector(final int capacity) {
        /* we assume 0 is absent from port IDs.
           we'll have some false negatives */
        this.entries = new int[capacity];
    }

    /**
     * Insert candidate if missing
     *
     * @param candidate   The entry we want to track
     * @param maximumHole Larger holes are ignored
     * @return The size of the hole (missing intermediate values)
     * between the previously smallest and newly smaller entry,
     * 0 if none or the previously smallest value was 0
     */
    int ensurePresent(int candidate, int maximumHole) {
        final int purgedOut, newFirst;
        synchronized (this.entries) {
            final int index = Arrays.binarySearch(entries, candidate);

            if (index >= 0) // found
                return 0;

            // Before: a b c d e f g
            // After:  b c X d e f g
            //               ^ ipoint
            final int ipoint = -1 - index;

            purgedOut = entries[0];
            System.arraycopy(entries, 1, entries, 0, ipoint - 2);
            newFirst = entries[0];

            entries[ipoint - 1] = candidate;
        }

        if (purgedOut != 0 && newFirst > purgedOut + 1) {
            final int hole = newFirst - purgedOut - 1;
            return hole > maximumHole ? hole : 0;
        } else {
            return 0;
        }
    }

    int countTotalHoles(int maximumHole) {
        int holes = 0;
        synchronized (this.entries) {
            for (int i = 0; i < this.entries.length - 1; i++) {
                final long current = this.entries[i];
                final long next = this.entries[i + 1];
                if (current != 0 && next != 0 && next > current + 1) {
                    final long hole = next - current - 1;
                    if (hole > maximumHole)
                        holes += hole;
                }
            }
        }
        return holes;
    }
}
