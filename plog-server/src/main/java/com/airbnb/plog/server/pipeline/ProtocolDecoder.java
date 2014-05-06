package com.airbnb.plog.server.pipeline;

import com.airbnb.plog.MessageImpl;
import com.airbnb.plog.server.commands.FourLetterCommand;
import com.airbnb.plog.server.fragmentation.Fragment;
import com.airbnb.plog.server.stats.StatisticsReporter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
public final class ProtocolDecoder extends MessageToMessageDecoder<DatagramPacket> {
    private final StatisticsReporter stats;

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out)
            throws Exception {
        final ByteBuf content = msg.content();
        final byte versionIdentifier = content.getByte(0);
        // versions are non-printable characters, push down the pipeline send as-is.
        if (versionIdentifier < 0 || versionIdentifier > 31) {
            log.debug("Unboxed UDP message");
            stats.receivedUdpSimpleMessage();
            msg.retain();
            out.add(new MessageImpl(content, null));
        } else if (versionIdentifier == 0) {
            final byte typeIdentifier = content.getByte(1);
            switch (typeIdentifier) {
                case 0:
                    final FourLetterCommand cmd = readCommand(msg);
                    if (cmd != null) {
                        log.debug("v0 command");
                        out.add(cmd);
                    } else
                        stats.receivedUnknownCommand();
                    break;
                case 1:
                    log.debug("v0 multipart message: {}", msg);
                    try {
                        final Fragment fragment = Fragment.fromDatagram(msg);
                        stats.receivedV0MultipartFragment(fragment.getFragmentIndex());
                        msg.retain();
                        out.add(fragment);
                    } catch (IllegalArgumentException e) {
                        log.error("Invalid header", e);
                        stats.receivedV0InvalidMultipartHeader();
                    }
                    break;
                default:
                    stats.receivedV0InvalidType();
            }
        } else {
            stats.receivedUdpInvalidVersion();
        }
    }

    private FourLetterCommand readCommand(DatagramPacket msg) {
        final ByteBuf content = msg.content();
        final int trailLength = content.readableBytes() - 6;
        if (trailLength < 0)
            return null;
        final byte[] trail = new byte[trailLength];
        final byte[] cmdBuff = new byte[4];
        content.getBytes(2, cmdBuff, 0, 4);
        content.getBytes(6, trail, 0, trail.length);
        return new FourLetterCommand(new String(cmdBuff), msg.sender(), trail);
    }
}
