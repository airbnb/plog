package com.airbnb.plog;

import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public class Message {
    private final ByteBuf payload;
}
