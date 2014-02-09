package com.airbnb.plog;

import com.google.common.cache.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/* TODO(pierre): much more instrumentation */

@Slf4j
public class PlogDefragmenter extends MessageToMessageDecoder<MultiPartMessageFragment> {
    private final StatisticsReporter stats;
    private final Cache<Long, PartialMultiPartMessage> incompleteMessages;

    public PlogDefragmenter(StatisticsReporter stats, int maxSize) {
        this.stats = stats;
        incompleteMessages = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .recordStats()
                .weigher(new Weigher<Long, PartialMultiPartMessage>() {
                    @Override
                    public int weigh(Long id, PartialMultiPartMessage msg) {
                        return msg.length();
                    }
                })
                .removalListener(new RemovalListener<Long, PartialMultiPartMessage>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, PartialMultiPartMessage> notification) {
                        // TODO(pierre): statistics! let's make sure we can discrimate complete messages
                        // from evictions with if (notification.wasEvicted())
                    }
                })
                .build();
    }

    public CacheStats getCacheStats() {
        return incompleteMessages.stats();
    }

    private synchronized PartialMultiPartMessage ingestIntoIncompleteMessage(MultiPartMessageFragment fragment) {
        final long id = fragment.getMsgId();
        final PartialMultiPartMessage fromMap = incompleteMessages.getIfPresent(id);
        if (fromMap != null) {
            fromMap.ingestFragment(fragment);
            if (fromMap.isComplete()) {
                log.debug("complete message");
                incompleteMessages.invalidate(fragment.getMsgId());
            } else {
                log.debug("incomplete message");
            }
            return fromMap;
        } else {
            PartialMultiPartMessage message = PartialMultiPartMessage.fromFragment(fragment);
            incompleteMessages.put(id, message);
            return message;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, MultiPartMessageFragment fragment, List<Object> out) throws Exception {
        PartialMultiPartMessage message;

        if (fragment.isAlone()) {
            log.debug("1-packet multipart message");
            out.add(fragment.getPayload());
            stats.receivedV0MultipartMessage();
        } else {
            log.debug("multipart message");
            message = ingestIntoIncompleteMessage(fragment);
            if (message.isComplete()) {
                out.add(message.getPayload());
                stats.receivedV0MultipartMessage();
            }
        }
    }

}
