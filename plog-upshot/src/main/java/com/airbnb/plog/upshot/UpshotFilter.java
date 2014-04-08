package com.airbnb.plog.upshot;

import com.airbnb.plog.Message;
import com.airbnb.plog.MessageImpl;
import com.airbnb.plog.filters.Filter;
import com.eclipsesource.json.JsonObject;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserFeature;
import com.foundationdb.sql.parser.StatementNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicLong;

class UpshotFilter extends SimpleChannelInboundHandler<Message> implements Filter {
    private static final RawValue UNKNOWN_SIGNATURE = ValueFactory.createRawValue("???");
    private static final int[] EMPTY_CARDINALITIES = new int[0];
    private static final MessagePack msgpack = new MessagePack();
    private static final int MINIMAL_FIELDS = 15;
    private final SQLParser parser = new SQLParser();
    private final AtomicLong invalidFormat = new AtomicLong(),
            invalidVersion = new AtomicLong(),
            parsingFailed = new AtomicLong();

    protected UpshotFilter() {
        super();
        parser.getFeatures().addAll(EnumSet.of(
                SQLParserFeature.INFIX_BIT_OPERATORS,
                SQLParserFeature.INFIX_LOGICAL_OPERATORS,
                SQLParserFeature.MYSQL_COLUMN_AS_FUNCS
        ));
    }

    private synchronized StatementNode parse(String sql) throws StandardException {
        return parser.parseStatement(sql);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        try {
            final Message transformed = transform(ctx, msg);

            if (transformed != null)
                ctx.fireChannelRead(transformed);
        } catch (IOException e) {
            invalidFormat.incrementAndGet();
        }
    }

    @Nullable
    protected Message transform(ChannelHandlerContext ctx, Message msg) throws IOException {
        final byte[] payload = msg.asBytes();

        final Value[] pack = msgpack.read(payload).asArrayValue().getElementArray();

        if (pack.length < MINIMAL_FIELDS) {
            invalidFormat.incrementAndGet();
            return null;
        }

        if (pack[0].asIntegerValue().getInt() != 0) {
            invalidVersion.incrementAndGet();
            return null;
        }

        final String sql = pack[14].asRawValue().getString();

        RawValue signature;
        int[] cardinalities;

        try {
            final SQLPrinter sqlPrinter = new SQLPrinter();
            signature = ValueFactory.createRawValue(sqlPrinter.toString(parse(sql)));
            cardinalities = sqlPrinter.getCardinalities();
        } catch (StandardException e) {
            signature = UNKNOWN_SIGNATURE;
            cardinalities = EMPTY_CARDINALITIES;
            parsingFailed.getAndIncrement();
        }

        pack[14] = signature;

        final BufferPacker packer = msgpack.createBufferPacker();
        packer.write(pack);
        packer.write(cardinalities);

        return MessageImpl.fromBytes(ctx.alloc(), packer.toByteArray());
    }

    @Override
    public JsonObject getStats() {
        return new JsonObject()
                .add("invalid_format", invalidFormat.get())
                .add("invalid_version", invalidVersion.get())
                .add("parsing_failed", parsingFailed.get());
    }

    @Override
    public String getName() {
        return "upshot";
    }
}
