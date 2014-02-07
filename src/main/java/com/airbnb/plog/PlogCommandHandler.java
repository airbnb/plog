package com.airbnb.plog;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

public class PlogCommandHandler extends SimpleChannelInboundHandler<PlogCommand> {
    public static final byte[] PONG_BYTES = "PONG".getBytes();
    private final Statistics stats;

    public PlogCommandHandler(Statistics stats) {
        this.stats = stats;
    }

    private DatagramPacket pong(PlogCommand ping) {
        final byte[] trail = ping.getTrail();
        int respLength = PONG_BYTES.length + trail.length;
        ByteBuf reply = Unpooled.buffer(respLength, respLength);
        reply.writeBytes(PONG_BYTES);
        reply.writeBytes(trail);
        return new DatagramPacket(reply, ping.getSender());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PlogCommand cmd) throws Exception {
        if (cmd.is(PlogCommand.KILL)) {
            System.err.println("KILL SWITCH!");
            System.exit(1);
        } else if (cmd.is(PlogCommand.PING)) {
            stats.receivedV0Command();
            ctx.writeAndFlush(pong(cmd));
        } else if (cmd.is(PlogCommand.STAT)) {
            stats.receivedV0Command();
            final ByteBuf resp = Unpooled.wrappedBuffer(stats.toJSON().getBytes(Charsets.UTF_8));
            ctx.writeAndFlush(new DatagramPacket(resp, cmd.getSender()));
        } else if (cmd.is(PlogCommand.ENVI)) {
            stats.receivedV0Command();
            // TODO: send config back
        } else {
            stats.receivedUnknownCommand();
        }
    }
}
