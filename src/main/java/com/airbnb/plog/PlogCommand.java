package com.airbnb.plog;

import java.net.InetSocketAddress;

public class PlogCommand {
    static final String PING = "PING";
    static final String STAT = "STAT";
    static final String KILL = "KILL";
    static final String ENVI = "ENVI";

    public String getCommand() {
        return command;
    }

    public byte[] getTrail() {
        return trail;
    }

    public InetSocketAddress getSender() {
        return sender;
    }

    PlogCommand(String command, InetSocketAddress sender, byte[] trail) {
        this.command = command.toUpperCase();
        this.sender = sender;
        this.trail = trail;
    }

    boolean is(String cmd) {
        return cmd.equals(this.getCommand());
    }

    private final String command;
    private final InetSocketAddress sender;
    private final byte[] trail;
}
