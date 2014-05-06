package com.airbnb.plog.server.packetloss;

import com.airbnb.plog.server.stats.StatisticsReporter;
import com.google.common.cache.*;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class ListenerHoleDetector {
    private final LoadingCache<Integer, PortHoleDetector> cache;
    private final StatisticsReporter stats;
    private final int maximumHole;

    public ListenerHoleDetector(final Config config, final StatisticsReporter stats) {
        final int portDetectorCapacity = config.getInt("ids_per_port");
        maximumHole = config.getInt("maximum_hole");

        this.cache = CacheBuilder.<Integer, PortHoleDetector>newBuilder()
                .maximumSize(config.getLong("ports"))
                .expireAfterAccess(
                        config.getDuration("expire_time", TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .recordStats()
                .removalListener(new RemovalListener<Integer, PortHoleDetector>() {
                    @Override
                    public void onRemoval(RemovalNotification<Integer, PortHoleDetector> notification) {
                        final PortHoleDetector detector = notification.getValue();
                        if (detector != null) {
                            final int holesFound = detector.countTotalHoles(maximumHole);
                            if (holesFound > 0)
                                stats.foundHolesFromDeadPort(holesFound);
                        }
                    }
                })
                .build(new CacheLoader<Integer, PortHoleDetector>() {
                    public PortHoleDetector load(Integer key) throws Exception {
                        return new PortHoleDetector(portDetectorCapacity);
                    }
                });
        this.stats = stats;
    }

    public int reportNewMessage(final long id) {
        final int clientPort = (int) (id >> Integer.SIZE);
        final int clientId = (int) (id & 0xffffffff);
        try {
            final int holesFound = this.cache.get(clientPort).ensurePresent(clientId, maximumHole);
            if (holesFound > 0) {
                stats.foundHolesFromNewMessage(holesFound);
            }
            return holesFound;
        } catch (ExecutionException e) {
            log.error("impossible is possible");
        }
        return 0; // still impossible
    }
}
