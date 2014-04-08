package com.airbnb.plog.console;

import com.airbnb.plog.Message;
import com.airbnb.plog.filters.Filter;
import com.eclipsesource.json.JsonObject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class ConsoleOutputFilter extends SimpleChannelInboundHandler<Message> implements Filter {
    final PrintStream target;
    final AtomicLong logged = new AtomicLong();

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        final byte[][] tags = msg.getTags();
        target.println(msg.toString());
        logged.incrementAndGet();
    }

    @Override
    public final JsonObject getStats() {
        return new JsonObject().add("logged", logged.get());
    }

    @Override
    public final String getName() {
        return "console";
    }
}
