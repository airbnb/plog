package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.charset.Charset;
import java.util.List;

public class PlogPdecoder extends MessageToMessageDecoder<DatagramPacket> {
    final Charset charset;

    PlogPdecoder(String charset) {
        this.charset = Charset.forName(charset);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
        final ByteBuf content = msg.content();
        final byte versionIdentifier = content.getByte(0);
        if (versionIdentifier < 0 || versionIdentifier > 31)
            out.add(msg.content().toString(charset));
        else if (versionIdentifier == 0) {
            final byte typeIdentifier = content.getByte(1);
            switch (typeIdentifier) {
                case 0:
                    throw new NotImplementedException();
                case 1:
                    out.add(MultiPartMessageFragment.fromDatagram(msg));
                default:
                    throw new IllegalArgumentException(
                            "Unknown packet typeIdentifier " + typeIdentifier +
                            " in version " + versionIdentifier);
            }
        } else {
            throw new IllegalArgumentException("Unknown version " + versionIdentifier);
        }
    }

    private MultiPartMessageFragment decode(DatagramPacket msg) {
        return null;
    }
}
