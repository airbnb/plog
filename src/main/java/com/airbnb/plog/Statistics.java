package com.airbnb.plog;

import java.util.concurrent.atomic.AtomicInteger;

public final class Statistics {
    private final AtomicInteger
            receivedTcpMessages = new AtomicInteger(),
            receivedUdpSimpleMessages = new AtomicInteger(),
            receivedUdpInvalidVersion = new AtomicInteger(),
            receivedV0InvalidType = new AtomicInteger(),
            receivedUnknownCommand = new AtomicInteger(),
            receivedV0Commands = new AtomicInteger(),
            receivedV0MultipartMessages = new AtomicInteger(),
            receivedV0MultipartFragments0 = new AtomicInteger(),
            receivedV0MultipartFragments1 = new AtomicInteger(),
            receivedV0MultipartFragments2 = new AtomicInteger(),
            receivedV0MultipartFragments3 = new AtomicInteger(),
            receivedV0MultipartFragments4to7 = new AtomicInteger(),
            receivedV0MultipartFragments8to15 = new AtomicInteger(),
            receivedV0MultipartFragments16to31 = new AtomicInteger(),
            receivedV0MultipartFragments32to63 = new AtomicInteger(),
            receivedV0MultipartFragments64to127 = new AtomicInteger(),
            receivedV0MultipartFragments128to255 = new AtomicInteger(),
            receivedV0MultipartFragments256to511 = new AtomicInteger(),
            receivedV0MultipartFragments512to1023 = new AtomicInteger(),
            receivedV0MultipartFragments1024to2047 = new AtomicInteger(),
            receivedV0MultipartFragments2048to4095 = new AtomicInteger(),
            receivedV0MultipartFragments4096to8191 = new AtomicInteger(),
            receivedV0MultipartFragments8192orMore = new AtomicInteger();

    public final int receivedTcpMessage() {
        return this.receivedTcpMessages.incrementAndGet();
    }

    public final int receivedUdpSimpleMessage() {
        return this.receivedUdpSimpleMessages.incrementAndGet();
    }

    public final int receivedUdpInvalidVersion() {
        return this.receivedUdpInvalidVersion.incrementAndGet();
    }

    public final int receivedV0InvalidType() {
        return this.receivedV0InvalidType.incrementAndGet();
    }

    public final int receivedV0Command() {
        return this.receivedV0Commands.incrementAndGet();
    }

    public final int receivedUnknownCommand() {
        return this.receivedUnknownCommand.incrementAndGet();
    }

    public final int receivedV0MultipartMessage() {
        return this.receivedV0MultipartMessages.incrementAndGet();
    }

    public final int receivedV0MultipartFragment(int index) {
        if (index == 0)
            return receivedV0MultipartFragments0.incrementAndGet();
        else if (index == 1)
            return receivedV0MultipartFragments1.incrementAndGet();
        else if (index == 2)
            return receivedV0MultipartFragments2.incrementAndGet();
        else if (index == 3)
            return receivedV0MultipartFragments3.incrementAndGet();
        else if (index < 8)
            return receivedV0MultipartFragments4to7.incrementAndGet();
        else if (index < 16)
            return receivedV0MultipartFragments8to15.incrementAndGet();
        else if (index < 32)
            return receivedV0MultipartFragments16to31.incrementAndGet();
        else if (index < 64)
            return receivedV0MultipartFragments32to63.incrementAndGet();
        else if (index < 128)
            return receivedV0MultipartFragments64to127.incrementAndGet();
        else if (index < 256)
            return receivedV0MultipartFragments128to255.incrementAndGet();
        else if (index < 512)
            return receivedV0MultipartFragments256to511.incrementAndGet();
        else if (index < 1024)
            return receivedV0MultipartFragments512to1023.incrementAndGet();
        else if (index < 2048)
            return receivedV0MultipartFragments1024to2047.incrementAndGet();
        else if (index < 4096)
            return receivedV0MultipartFragments2048to4095.incrementAndGet();
        else if (index < 8192)
            return receivedV0MultipartFragments4096to8191.incrementAndGet();
        else
            return receivedV0MultipartFragments8192orMore.incrementAndGet();
    }

    @Override
    public final String toString() {
        return "{\"tcpMessages\":" + this.receivedTcpMessages.get() +
                ", \"udpSimpleMessages\":" + this.receivedUdpSimpleMessages.get() +
                ", \"udpInvalidVersion\":" + this.receivedUdpInvalidVersion.get() +
                ", \"v0InvalidType\":" + this.receivedV0InvalidType.get() +
                ", \"unkownCommand\":" + this.receivedUnknownCommand.get() +
                ", \"v0Commands\":" + this.receivedV0Commands.get() +
                ", \"v0MultipartMessages\":" + this.receivedV0MultipartMessages.get() +
                ", \"v0MultipartFragments0\":" + this.receivedV0MultipartFragments0.get() +
                ", \"v0MultipartFragments1\":" + this.receivedV0MultipartFragments1.get() +
                ", \"v0MultipartFragments2\":" + this.receivedV0MultipartFragments2.get() +
                ", \"v0MultipartFragments3\":" + this.receivedV0MultipartFragments3.get() +
                ", \"v0MultipartFragments4to7\":" + this.receivedV0MultipartFragments4to7.get() +
                ", \"v0MultipartFragments8to15\":" + this.receivedV0MultipartFragments8to15.get() +
                ", \"v0MultipartFragments16to31\":" + this.receivedV0MultipartFragments16to31.get() +
                ", \"v0MultipartFragments32to63\":" + this.receivedV0MultipartFragments32to63.get() +
                ", \"v0MultipartFragments64to127\":" + this.receivedV0MultipartFragments64to127.get() +
                ", \"v0MultipartFragments128to255\":" + this.receivedV0MultipartFragments128to255.get() +
                ", \"v0MultipartFragments256to511\":" + this.receivedV0MultipartFragments256to511.get() +
                ", \"v0MultipartFragments512to1023\":" + this.receivedV0MultipartFragments512to1023.get() +
                ", \"v0MultipartFragments1024to2047\":" + this.receivedV0MultipartFragments1024to2047.get() +
                ", \"v0MultipartFragments2048to4095\":" + this.receivedV0MultipartFragments2048to4095.get() +
                ", \"v0MultipartFragments4096to8191\":" + this.receivedV0MultipartFragments4096to8191.get() +
                ", \"v0MultipartFragments8192orMore\":" + this.receivedV0MultipartFragments8192orMore.get() +
                "}";
    }

    public final String toJSON() {
        return this.toString();
    }
}
