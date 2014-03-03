package com.airbnb.plog.sinks;

import com.airbnb.plog.Message;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class Sink extends SimpleChannelInboundHandler<Message> {
}
