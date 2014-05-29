package com.airbnb.plog.server.fragmentation;

import com.airbnb.plog.MessageImpl;
import com.airbnb.plog.common.Murmur3;
import com.airbnb.plog.server.packetloss.ListenerHoleDetector;
import com.airbnb.plog.server.stats.StatisticsReporter;
import com.google.common.cache.*;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class Defragmenter extends MessageToMessageDecoder<Fragment> {
    private final StatisticsReporter stats;
    private final Cache<Long, FragmentedMessage> incompleteMessages;
    private final ListenerHoleDetector detector;

    public Defragmenter(final StatisticsReporter statisticsReporter, final Config config) {
        this.stats = statisticsReporter;

        final Config holeConfig = config.getConfig("detect_holes");
        if (holeConfig.getBoolean("enabled")) {
            detector = new ListenerHoleDetector(holeConfig, stats);
        } else {
            detector = null;
        }

        incompleteMessages = CacheBuilder.newBuilder()
                .maximumWeight(config.getInt("max_size"))
                .expireAfterAccess(config.getDuration("expire_time", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .recordStats()
                .weigher(new Weigher<Long, FragmentedMessage>() {
                    @Override
                    public int weigh(Long id, FragmentedMessage msg) {
                        return msg.getContentLength();
                    }
                })
                .removalListener(new RemovalListener<Long, FragmentedMessage>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, FragmentedMessage> notification) {
                        if (notification.getCause() == RemovalCause.EXPLICIT) {
                            return;
                        }

                        final FragmentedMessage message = notification.getValue();
                        if (message == null) {
                            return; // cannot happen with this cache, holds strong refs.
                        }

                        final int fragmentCount = message.getFragmentCount();
                        final BitSet receivedFragments = message.getReceivedFragments();
                        for (int idx = 0; idx < fragmentCount; idx++) {
                            if (!receivedFragments.get(idx)) {
                                stats.missingFragmentInDroppedMessage(idx, fragmentCount);
                            }
                        }
                        message.release();
                    }
                }).build();
    }

    public CacheStats getCacheStats() {
        return incompleteMessages.stats();
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final Fragment fragment, final List<Object> out)
            throws Exception {
        if (fragment.isAlone()) {
            if (detector != null) {
                detector.reportNewMessage(fragment.getMsgId());
            }

            final ByteBuf payload = fragment.content();
            final int computedHash = Murmur3.hash32(payload);

            if (computedHash == fragment.getMsgHash()) {
                payload.retain();
                out.add(new MessageImpl(payload, fragment.getTags()));
                this.stats.receivedV0MultipartMessage();
            } else {
                this.stats.receivedV0InvalidChecksum(1);
            }
        } else {
            handleMultiFragment(fragment, out);
        }
    }

    private void handleMultiFragment(final Fragment fragment, List<Object> out) throws java.util.concurrent.ExecutionException {
        // 2 fragments or more
        final long msgId = fragment.getMsgId();
        final boolean[] isNew = {false};
        final boolean complete;

        final FragmentedMessage message = incompleteMessages.get(msgId, new Callable<FragmentedMessage>() {
            @Override
            public FragmentedMessage call() throws Exception {
                isNew[0] = true;

                if (detector != null) {
                    detector.reportNewMessage(fragment.getMsgId());
                }

                return FragmentedMessage.fromFragment(fragment, Defragmenter.this.stats);
            }
        });

        if (isNew[0]) {
            complete = false; // new 2+ fragments, so cannot be complete
        } else {
            complete = message.ingestFragment(fragment, this.stats);
        }

        if (complete) {
            incompleteMessages.invalidate(fragment.getMsgId());

            final ByteBuf payload = message.getPayload();

            if (Murmur3.hash32(payload) == message.getChecksum()) {
                out.add(new MessageImpl(payload, message.getTags()));
                this.stats.receivedV0MultipartMessage();
            } else {
                message.release();
                this.stats.receivedV0InvalidChecksum(message.getFragmentCount());
            }
        }
    }
}
