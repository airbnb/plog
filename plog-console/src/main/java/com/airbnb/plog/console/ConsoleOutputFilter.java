package com.airbnb.plog.console;

import com.airbnb.plog.Message;
import com.airbnb.plog.filters.Filter;
import com.google.common.base.Joiner;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

import java.io.PrintStream;

@RequiredArgsConstructor
public class ConsoleOutputFilter extends SimpleChannelInboundHandler<Message> implements Filter {
    final PrintStream target;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        final byte[][] tags = msg.getTags();
        if (tags == null) {
            target.println(new String(msg.asBytes()));
        } else {
            final String tagList = Joiner.on(',').join(tags);
            target.println("[" + tagList + "] " + new String(msg.asBytes()));
        }
    }
}
