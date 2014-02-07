package com.airbnb.plog;

import lombok.Getter;
import lombok.ToString;

import java.net.InetSocketAddress;

@ToString
public class PlogCommand {
    public static final String PING = "PING";
    public static final String STAT = "STAT";
    public static final String KILL = "KILL";
    public static final String ENVI = "ENVI";

    @Getter
    private final String command;
    @Getter
    private final InetSocketAddress sender;
    @Getter
    private final byte[] trail;

    PlogCommand(String command, InetSocketAddress sender, byte[] trail) {
        this.command = command.toUpperCase();
        this.sender = sender;
        this.trail = trail;
    }

    boolean is(String cmd) {
        return cmd.equals(this.getCommand());
    }
}
