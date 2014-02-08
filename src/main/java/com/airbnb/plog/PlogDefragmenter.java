package com.airbnb.plog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class PlogDefragmenter extends MessageToMessageDecoder<MultiPartMessageFragment> {
    private final StatisticsReporter stats;
    private final Cache<Long, PartialMultiPartMessage> incompleteMessages;


    public PlogDefragmenter(StatisticsReporter stats, int maxSize) {
        this.stats = stats;
        incompleteMessages = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher(new Weigher<Long, PartialMultiPartMessage>() {
                    @Override
                    public int weigh(Long id, PartialMultiPartMessage msg) {
                        return msg.length();
                    }
                }).build();
    }

    private synchronized PartialMultiPartMessage ingestIntoIncompleteMessage(MultiPartMessageFragment fragment) {
        final long id = fragment.getMsgId();
        final PartialMultiPartMessage fromMap = incompleteMessages.getIfPresent(id);
        if (fromMap != null) {
            fromMap.ingestFragment(fragment);
            if (fromMap.isComplete())
                incompleteMessages.invalidate(fragment.getMsgId());
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
            out.add(fragment.getPayload());
            stats.receivedV0MultipartMessage();
        } else {
            message = ingestIntoIncompleteMessage(fragment);
            if (message.isComplete()) {
                out.add(message.getPayload());
                stats.receivedV0MultipartMessage();
            }
        }
    }
}
