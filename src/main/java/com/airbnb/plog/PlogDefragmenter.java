package com.airbnb.plog;

import com.google.common.cache.*;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.BitSet;
import java.util.List;

/* TODO(pierre): much more instrumentation */

@Slf4j
public class PlogDefragmenter extends MessageToMessageDecoder<MultiPartMessageFragment> {
    private final StatisticsReporter stats;
    private final Cache<Long, PartialMultiPartMessage> incompleteMessages;

    public PlogDefragmenter(final StatisticsReporter stats, int maxSize) {
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
                        final PartialMultiPartMessage message = notification.getValue();
                        if (message != null) {
                            final int fragmentCount = message.getFragmentCount();
                            final BitSet receivedFragments = message.getReceivedFragments();
                            for (int idx = 0; idx < fragmentCount; idx++)
                                if (!receivedFragments.get(idx))
                                    stats.missingFragmentInDroppedMultiPartMessage(idx, fragmentCount);
                        } else {
                            // let's use the magic value fragment 0, expected fragments 0 if the message was GC'ed,
                            // as it wouldn't happen otherwise
                            stats.missingFragmentInDroppedMultiPartMessage(0, 0);
                        }
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
            fromMap.ingestFragment(fragment, this.stats);
            if (fromMap.isComplete()) {
                log.debug("complete message");
                incompleteMessages.invalidate(fragment.getMsgId());
            } else {
                log.debug("incomplete message");
            }
            return fromMap;
        } else {
            PartialMultiPartMessage message = PartialMultiPartMessage.fromFragment(fragment, this.stats);
            incompleteMessages.put(id, message);
            return message;
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, MultiPartMessageFragment fragment, List<Object> out) throws Exception {
        PartialMultiPartMessage message;

        if (fragment.isAlone()) {
            pushPayloadIfValid(fragment.getPayload(), fragment.getMsgHash(), 1, out);
        } else {
            message = ingestIntoIncompleteMessage(fragment);
            if (message.isComplete())
                pushPayloadIfValid(message.getPayload(), message.getHash(), message.getFragmentCount(), out);
        }
    }

    private void pushPayloadIfValid(final ByteBuf payload,
                                    final int expectedHash,
                                    final int fragmentCount,
                                    List<Object> out) {
        final byte[] bytes = ByteBufs.toByteArray(payload);
        final int computedHash = Hashing.murmur3_32().hashBytes(bytes).asInt();
        if (computedHash == expectedHash) {
            out.add(new Message(bytes));
            this.stats.receivedV0MultipartMessage();
        } else {
            log.warn("Client sent hash {}, not matching computed hash {} for bytes {} (fragment count {})",
                    expectedHash, computedHash, bytes, fragmentCount);
            this.stats.receivedV0InvalidChecksum(fragmentCount);
        }
    }
}
