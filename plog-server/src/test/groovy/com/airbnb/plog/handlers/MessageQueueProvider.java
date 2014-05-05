package com.airbnb.plog.handlers;

import com.airbnb.plog.Message;
import com.eclipsesource.json.JsonObject;
import com.google.common.collect.Queues;
import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.Getter;

import java.util.Queue;

public class MessageQueueProvider implements HandlerProvider {
    @Getter
    private static final Queue<Message> queue = Queues.newArrayDeque();

    @Override
    public Handler getHandler(Config config) throws Exception {
        return new MessageQueueHandler();
    }

    private static class MessageQueueHandler extends SimpleChannelInboundHandler<Message> implements Handler {
        @Override
        public JsonObject getStats() {
            return new JsonObject().add("queued", queue.size());
        }

        @Override
        public String getName() {
            return "mqueue";
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
            msg.retain();
            queue.add(msg);
        }
    }
}
