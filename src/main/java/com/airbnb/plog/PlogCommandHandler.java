package com.airbnb.plog;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class PlogCommandHandler extends MessageToMessageDecoder<PlogCommand> {
    private final Statistics stats;

    public PlogCommandHandler(Statistics stats) {
        this.stats = stats;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof PlogCommand;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PlogCommand msg, List<Object> out) throws Exception {
        if (msg.is(PlogCommand.ENVI)) {
                /* TODO: send config back */
        } else if (msg.is(PlogCommand.KILL)) {
            System.exit(1);
        } else if (msg.is(PlogCommand.PING)) {
                /* TODO: send pong back */
        } else if (msg.is(PlogCommand.STAT)) {
                /* TODO: send stats back */
        } else {
            stats.receivedUnknownCommand();
        }
    }
}
