package com.airbnb.plog.server.packetloss;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
final class PortHoleDetector {
    @Getter(AccessLevel.PACKAGE)
    private final int[] entries;
    @Getter(AccessLevel.PACKAGE)
    private long minSeen;
    @Getter(AccessLevel.PACKAGE)
    private long maxSeen;

    PortHoleDetector(final int capacity) {
        /* we assume Integer.MIN_VALUE is absent from port IDs.
           we'll have some false negatives */
        if (capacity < 1)
            throw new IllegalArgumentException("Insufficient capacity " + capacity);
        this.entries = new int[capacity];
        reset(null);
    }

    private void reset(Integer value) {
        if (value != null)
            log.info("Resetting {} for {}", this.entries, value);
        minSeen = Long.MAX_VALUE;
        maxSeen = Long.MIN_VALUE;
        Arrays.fill(this.entries, Integer.MIN_VALUE);
    }

    /**
     * Insert candidate if missing
     *
     * @param candidate The entry we want to track
     * @param maxHole   Larger holes are ignored
     * @return The size of the hole (missing intermediate values)
     * between the previously smallest and newly smallest entry
     */
    int ensurePresent(int candidate, int maxHole) {
        if (maxHole < 1)
            throw new MaxHoleTooSmall(maxHole);

        final int purgedOut, newFirst;
        synchronized (this.entries) {
            // solve port reuse
            if (candidate < minSeen) {
                if (minSeen != Long.MAX_VALUE && minSeen - candidate > maxHole)
                    reset(candidate);
                else
                    minSeen = candidate;
            }

            if (candidate > maxSeen) {
                if (maxSeen != Long.MIN_VALUE && candidate - maxSeen > maxHole)
                    reset(candidate);
                else
                    maxSeen = candidate;
            }

            final int index = Arrays.binarySearch(entries, candidate);

            if (index >= 0) // found
                return 0;

            //            index = (-(ipoint) - 1)
            // <=>    index + 1 = -(ipoint)
            // <=> -(index + 1) = ipoint
            final int ipoint = -1 - index;

            // Before: a b c d e f g
            // After:  b c X d e f g
            //               ^ ipoint

            if (ipoint == 0) {
                purgedOut = candidate;
                newFirst = entries[0];
            } else {
                purgedOut = entries[0];
                if (ipoint > 1)
                    System.arraycopy(entries, 1, entries, 0, ipoint - 1);
                entries[ipoint - 1] = candidate;
                newFirst = entries[0];
            }
        }


        // magical value
        if (purgedOut == Integer.MIN_VALUE)
            return 0;

        final int hole = newFirst - purgedOut - 1;
        if (hole > 0) {
            if (hole <= maxHole) {
                log.info("Pushed out hole between {} and {}", purgedOut, newFirst);
                return hole;
            } else {
                log.info("Pushed out and ignored hole between {} and {}", purgedOut, newFirst);
                return 0;
            }
        } else if (hole < 0) {
            log.warn("Negative hole pushed out between {} and {} ({})",
                    purgedOut, newFirst, this.entries);
        }
        return 0;
    }

    int countTotalHoles(int maxHole) {
        if (maxHole < 1)
            throw new MaxHoleTooSmall(maxHole);

        int holes = 0;
        synchronized (this.entries) {
            for (int i = 0; i < this.entries.length - 1; i++) {
                final long current = this.entries[i];
                final long next = this.entries[i + 1];

                // magical values
                if (current == Integer.MIN_VALUE || next == Integer.MIN_VALUE)
                    continue;

                final long hole = next - current - 1;
                if (hole > 0) {
                    if (hole <= maxHole) {
                        log.info("Scanned hole between {} and {}", hole, current, next);
                        holes += hole;
                    } else {
                        log.info("Scanned and ignored hole between {} and {}", current, next);
                    }
                } else if (hole < 0) {
                    log.warn("Scanned through negative hole between {} and {} ({})",
                            current, next, this.entries);
                }
            }
        }
        return holes;
    }

    @RequiredArgsConstructor
    static final class MaxHoleTooSmall extends IllegalArgumentException {
        final int maximumHole;

        @Override
        public String getMessage() {
            return "Maximum hole too small: " + maximumHole;
        }
    }
}
