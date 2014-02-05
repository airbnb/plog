package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.nio.charset.Charset;
import java.util.List;

public final class PlogPdecoder extends MessageToMessageDecoder<DatagramPacket> {
    private final Charset charset;
    private final Statistics stats;

    PlogPdecoder(Charset charset, Statistics stats) {
        this.charset = charset;
        this.stats = stats;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out)
            throws Exception {
        final ByteBuf content = msg.content();
        final byte versionIdentifier = content.getByte(0);
        if (versionIdentifier < 0 || versionIdentifier > 31)
            out.add(msg.content().toString(charset));
        else if (versionIdentifier == 0) {
            final byte typeIdentifier = content.getByte(1);
            switch (typeIdentifier) {
                case 0:
                    final byte[] cmdBuff = new byte[4];
                    try {
                        content.getBytes(1, cmdBuff, 0, 4);
                        out.add(new PlogCommand(new String(cmdBuff)));
                    } catch (IndexOutOfBoundsException e) {
                        stats.receivedUnknownCommand();
                    }
                case 1:
                    final MultiPartMessageFragment fragment = MultiPartMessageFragment.fromDatagram(msg);
                    stats.receivedV0MultipartFragment(fragment.getPacketIndex());
                    out.add(fragment);
                default:
                    stats.receivedV0InvalidType();
            }
        } else {
            stats.receivedUdpInvalidVersion();
        }
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof DatagramPacket;
    }
}
