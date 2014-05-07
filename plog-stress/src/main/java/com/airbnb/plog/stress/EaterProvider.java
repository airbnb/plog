package com.airbnb.plog.stress;

import com.airbnb.plog.Message;
import com.airbnb.plog.handlers.Handler;
import com.airbnb.plog.handlers.HandlerProvider;
import com.eclipsesource.json.JsonObject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.atomic.AtomicLong;

public class EaterProvider implements HandlerProvider {
    @Override
    public Handler getHandler(Config config) throws Exception {
        return new Eater();
    }

    private static class Eater extends SimpleChannelInboundHandler<Message> implements Handler {
        final AtomicLong counter = new AtomicLong();

        @Override
        public JsonObject getStats() {
            return new JsonObject().add("seen_messages", counter.get());
        }

        @Override
        public String getName() {
            return "eater";
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
            counter.incrementAndGet();
        }
    }
}
