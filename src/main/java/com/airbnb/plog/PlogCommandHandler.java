package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class PlogCommandHandler extends MessageToMessageDecoder<PlogCommand> {
    public static final byte[] PONG_BYTES = "PONG".getBytes();
    private final Statistics stats;

    public PlogCommandHandler(Statistics stats) {
        this.stats = stats;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PlogCommand cmd, List<Object> out) throws Exception {
        if (cmd.is(PlogCommand.ENVI)) {
                /* TODO: send config back */
        } else if (cmd.is(PlogCommand.KILL)) {
            System.exit(1);
        } else if (cmd.is(PlogCommand.PING)) {
            ctx.writeAndFlush(pong(cmd));
        } else if (cmd.is(PlogCommand.STAT)) {
            /* TODO: send stats */
        } else {
            stats.receivedUnknownCommand();
        }
    }

    private DatagramPacket pong(PlogCommand ping) {
        final byte[] trail = ping.getTrail();
        int respLength = PONG_BYTES.length + trail.length;
        ByteBuf reply = Unpooled.buffer(respLength, respLength);
        reply.writeBytes(PONG_BYTES);
        reply.writeBytes(trail);
        return new DatagramPacket(reply, ping.getSender());
    }
}
