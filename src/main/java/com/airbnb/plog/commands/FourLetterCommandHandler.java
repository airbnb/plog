package com.airbnb.plog.commands;

import com.airbnb.plog.stats.SimpleStatisticsReporter;
import com.google.common.base.Charsets;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class FourLetterCommandHandler extends SimpleChannelInboundHandler<FourLetterCommand> {
    private static final byte[] PONG_BYTES = "PONG".getBytes();
    private final SimpleStatisticsReporter stats;
    private final Config config;

    private DatagramPacket pong(FourLetterCommand ping) {
        final byte[] trail = ping.getTrail();
        int respLength = PONG_BYTES.length + trail.length;
        ByteBuf reply = Unpooled.buffer(respLength, respLength);
        reply.writeBytes(PONG_BYTES);
        reply.writeBytes(trail);
        return new DatagramPacket(reply, ping.getSender());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FourLetterCommand cmd) throws Exception {
        if (cmd.is(FourLetterCommand.KILL)) {
            log.warn("KILL SWITCH!");
            System.exit(1);
        } else if (cmd.is(FourLetterCommand.PING)) {
            ctx.writeAndFlush(pong(cmd));
            stats.receivedV0Command();
        } else if (cmd.is(FourLetterCommand.STAT)) {
            reply(ctx, cmd, stats.toJSON());
            stats.receivedV0Command();
        } else if (cmd.is(FourLetterCommand.ENVI)) {
            reply(ctx, cmd, config.toString());
            stats.receivedV0Command();
        } else {
            stats.receivedUnknownCommand();
        }
    }

    private void reply(ChannelHandlerContext ctx, FourLetterCommand cmd, String response) {
        final ByteBuf payload = Unpooled.wrappedBuffer(response.getBytes(Charsets.UTF_8));
        final DatagramPacket packet = new DatagramPacket(payload, cmd.getSender());
        ctx.writeAndFlush(packet);
    }
}
