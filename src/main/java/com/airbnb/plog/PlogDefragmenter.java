package com.airbnb.plog;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.Data;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PlogDefragmenter extends MessageToMessageDecoder<MultiPartMessageFragment> {
    private final StatisticsReporter stats;
    private final Map<Long, IncomingMultiPartMessage> incompleteMessages;
    private final Charset charset;

    public PlogDefragmenter(Charset charset, StatisticsReporter stats, Config config) {
        this.charset = charset;
        this.stats = stats;
        incompleteMessages = new CacheBuilder<Long, IncomingMultiPartMessage>()
                .maximumWeight(config.getInt("max_size"))
                .weigher(new Weigher<Long, IncomingMultiPartMessage>() {
                    @Override
                    public int weigh(Long id, IncomingMultiPartMessage msg) {
                        return msg.length();
                    }
                }).build();

    }

    private long identifierFor(MultiPartMessageFragment fragment) {
        return fragment.getMsgId() + (2 ^ 32) * fragment.getMsgPort();
    }

    private synchronized IngestionResult ingestIntoIncompleteMessage(MultiPartMessageFragment fragment) {
        final long id = identifierFor(fragment);
        final IncomingMultiPartMessage fromMap = incompleteMessages.get(id);
        if (fromMap != null) {
            fromMap.ingestFragment(fragment);
            return new IngestionResult(false, id, fromMap);
        } else {
            IncomingMultiPartMessage message = IncomingMultiPartMessage.fromFragment(fragment);
            insertInMap(id, message);
            return new IngestionResult(true, id, message);
        }
    }

    private IncomingMultiPartMessage fragmentToMessage(MultiPartMessageFragment fragment) {
        if (fragment.isAlone())
            return IncomingMultiPartMessage.fromFragment(fragment);

        /* Actually multi-part */
        final IngestionResult ingestion = ingestIntoIncompleteMessage(fragment);

        if (ingestion.isInsertion())
            while (payloadSize.get() > maxPayloadSize)
                removeFromMap(incompleteMessages.entrySet().iterator().next().getKey());
        else if (ingestion.getMessage().isComplete())
            removeFromMap(ingestion.getId());

        return ingestion.getMessage();
    }

    private void removeFromMap(long id) {
        final IncomingMultiPartMessage removed = incompleteMessages.remove(id);
        if (removed != null)
            payloadSize.addAndGet(-1 * removed.length());
    }

    private void insertInMap(long id, IncomingMultiPartMessage message) {
        incompleteMessages.put(id, message);
        payloadSize.addAndGet(message.length());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, MultiPartMessageFragment fragment, List<Object> out) throws Exception {
        final IncomingMultiPartMessage msg = fragmentToMessage(fragment);
        if (msg.isComplete()) {
            final ByteBuf payload = msg.getPayload();
            out.add(payloadToString(payload));
            stats.receivedV0MultipartMessage();
        }
    }

    private String payloadToString(ByteBuf payload) {
        final int length = payload.readableBytes();
        final byte[] bytes = new byte[length];
        payload.readBytes(bytes, 0, length);
        return new String(bytes, charset);
    }

    @Data
    private static final class IngestionResult {
        final boolean insertion;
        final long id;
        final IncomingMultiPartMessage message;
    }
}
