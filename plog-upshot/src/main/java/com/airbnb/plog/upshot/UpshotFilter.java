package com.airbnb.plog.upshot;

import com.airbnb.plog.Message;
import com.airbnb.plog.MessageImpl;
import com.airbnb.plog.filters.Filter;
import com.eclipsesource.json.JsonObject;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

class UpshotFilter extends SimpleChannelInboundHandler<Message> implements Filter {
    private static final RawValue UNKNOWN_SIGNATURE = ValueFactory.createRawValue("???");
    private static final int[] EMPTY_CARDINALITIES = new int[0];
    private static final MessagePack msgpack = new MessagePack();

    private final AtomicLong invalidFormat = new AtomicLong(),
            invalidVersion = new AtomicLong(),
            parsingFailed = new AtomicLong();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
            throws FormatException, IOException {
        final byte[] payload = msg.asBytes();

        final Value[] pack = msgpack.read(payload).asArrayValue().getElementArray();

        final SQLParser parser = new SQLParser();

        if (pack.length < 15) {
            invalidFormat.incrementAndGet();
            return;
        }

        if (pack[0].asIntegerValue().getInt() != 0) {
            invalidVersion.incrementAndGet();
            return;
        }

        final String sql = pack[14].asRawValue().getString();

        RawValue signature;
        int[] cardinalities;

        try {
            final StatementNode rootNode = parser.parseStatement(sql);
            final SQLPrinter sqlPrinter = new SQLPrinter();
            signature = ValueFactory.createRawValue(sqlPrinter.toString(rootNode));
            cardinalities = sqlPrinter.getCardinalities();
        } catch (StandardException e) {
            signature = UNKNOWN_SIGNATURE;
            cardinalities = EMPTY_CARDINALITIES;
            parsingFailed.incrementAndGet();
        }

        pack[14] = signature;

        final BufferPacker packer = msgpack.createBufferPacker();
        packer.write(pack);
        packer.write(cardinalities);

        ctx.fireChannelRead(MessageImpl.fromBytes(ctx.alloc(), packer.toByteArray()));
    }

    @Override
    public JsonObject getStats() {
        final JsonObject stats = new JsonObject();
        stats.add("invalid_format", invalidFormat.get());
        stats.add("invalid_version", invalidVersion.get());
        stats.add("parsing_failed", parsingFailed.get());
        return stats;
    }
}
