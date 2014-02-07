package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.nio.charset.Charset;
import java.util.List;

public final class PlogPDecoder extends MessageToMessageDecoder<DatagramPacket> {
    private final Charset charset;
    private final Statistics stats;

    PlogPDecoder(Charset charset, Statistics stats) {
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
                    final PlogCommand e = readCommand(msg);
                    if (e != null)
                        out.add(e);
                    else
                        stats.receivedUnknownCommand();
                    break;
                case 1:
                    final MultiPartMessageFragment fragment = MultiPartMessageFragment.fromDatagram(msg);
                    stats.receivedV0MultipartFragment(fragment.getFragmentIndex());
                    out.add(fragment);
                    break;
                default:
                    stats.receivedV0InvalidType();
            }
        } else {
            stats.receivedUdpInvalidVersion();
        }
    }

    private PlogCommand readCommand(DatagramPacket msg) {
        final byte[] cmdBuff = new byte[4];
        final ByteBuf content = msg.content();
        final int trailLength = content.readableBytes() - 6;
        final byte[] trail = new byte[trailLength];
        try {
            content.getBytes(2, cmdBuff, 0, 4);
            content.getBytes(6, trail, 0, trail.length);
            return new PlogCommand(new String(cmdBuff), msg.sender(), trail);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }
}
