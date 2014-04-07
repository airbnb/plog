package com.airbnb.plog;

import io.netty.buffer.ByteBufHolder;

public interface Message extends ByteBufHolder {
    byte[] asBytes();
    byte[][] getTags();
}
