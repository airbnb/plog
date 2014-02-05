package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.charset.Charset;
import java.util.List;

public class PlogPdecoder extends MessageToMessageDecoder<DatagramPacket> {
    private final Charset charset;
    private final Statistics stats;

    PlogPdecoder(String charset, Statistics stats) {
        this.charset = Charset.forName(charset);
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
                    stats.receivedV0Command();
                    throw new NotImplementedException();
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
}
