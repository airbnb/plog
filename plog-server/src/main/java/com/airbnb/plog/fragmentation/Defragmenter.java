package com.airbnb.plog.fragmentation;

import com.airbnb.plog.Message;
import com.airbnb.plog.packetloss.ListenerHoleDetector;
import com.airbnb.plog.stats.StatisticsReporter;
import com.airbnb.plog.utils.ByteBufs;
import com.google.common.cache.*;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Defragmenter extends MessageToMessageDecoder<Fragment> {
    private final StatisticsReporter stats;
    private final Cache<Long, FragmentedMessage> incompleteMessages;
    private final ListenerHoleDetector detector;

    public Defragmenter(final StatisticsReporter statisticsReporter, Config config) {
        this.stats = statisticsReporter;

        final Config holeConfig = config.getConfig("detect_holes");
        if (holeConfig.getBoolean("enabled"))
            detector = new ListenerHoleDetector(holeConfig, stats);
        else
            detector = null;

        incompleteMessages = CacheBuilder.newBuilder()
                .maximumWeight(config.getInt("max_size"))
                .expireAfterAccess(config.getDuration("expire_time", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .recordStats()
                .weigher(new Weigher<Long, FragmentedMessage>() {
                    @Override
                    public int weigh(Long id, FragmentedMessage msg) {
                        return msg.getPayloadLength();
                    }
                })
                .removalListener(new RemovalListener<Long, FragmentedMessage>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, FragmentedMessage> notification) {
                        final FragmentedMessage message = notification.getValue();
                        if (message != null) {
                            final int fragmentCount = message.getFragmentCount();
                            final BitSet receivedFragments = message.getReceivedFragments();
                            for (int idx = 0; idx < fragmentCount; idx++)
                                if (!receivedFragments.get(idx))
                                    stats.missingFragmentInDroppedMessage(idx, fragmentCount);
                        } else {
                            // let's use the magic value fragment 0, expected fragments 0 if the message was GC'ed,
                            // as it wouldn't happen otherwise
                            stats.missingFragmentInDroppedMessage(0, 0);
                        }
                    }
                })
                .build();
    }

    public CacheStats getCacheStats() {
        return incompleteMessages.stats();
    }

    private synchronized FragmentedMessage ingestIntoIncompleteMessage(Fragment fragment) {
        final long id = fragment.getMsgId();
        final FragmentedMessage fromMap = incompleteMessages.getIfPresent(id);
        if (fromMap != null) {
            fromMap.ingestFragment(fragment, this.stats);
            if (fromMap.isComplete()) {
                log.debug("complete message");
                incompleteMessages.invalidate(fragment.getMsgId());
            } else {
                log.debug("incomplete message");
            }
            return fromMap;
        } else {
            if (detector != null)
                detector.reportNewMessage(fragment.getMsgId());
            FragmentedMessage message = FragmentedMessage.fromFragment(fragment, this.stats);
            incompleteMessages.put(id, message);
            return message;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Fragment fragment, List<Object> out) throws Exception {
        FragmentedMessage message;
        log.debug("Defragmenting {}", fragment);
        if (fragment.isAlone()) {
            if (detector != null)
                detector.reportNewMessage(fragment.getMsgId());
            pushPayloadIfValid(fragment.getPayload(), fragment.getMsgHash(), 1, out);
        } else {
            message = ingestIntoIncompleteMessage(fragment);
            if (message.isComplete())
                pushPayloadIfValid(message.getPayload(), message.getChecksum(), message.getFragmentCount(), out);
        }
    }

    private void pushPayloadIfValid(final ByteBuf payload,
                                    final int expectedHash,
                                    final int fragmentCount,
                                    List<Object> out) {
        final byte[] bytes = ByteBufs.toByteArray(payload);
        final int computedHash = Hashing.murmur3_32().hashBytes(bytes).asInt();
        if (computedHash == expectedHash) {
            out.add(new Message(payload));
            this.stats.receivedV0MultipartMessage();
        } else {
            log.warn("Client sent hash {}, not matching computed hash {} for bytes {} (fragment count {})",
                    expectedHash, computedHash, BaseEncoding.base16().encode(bytes), fragmentCount);
            this.stats.receivedV0InvalidChecksum(fragmentCount);
        }
    }
}
