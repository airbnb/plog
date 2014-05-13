package com.airbnb.plog.console;

import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.eclipsesource.json.JsonObject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public final class ConsoleOutputHandler extends SimpleChannelInboundHandler<Message> implements Handler {
    private final PrintStream target;
    private final AtomicLong logged = new AtomicLong();

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
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
