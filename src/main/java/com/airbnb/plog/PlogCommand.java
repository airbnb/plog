package com.airbnb.plog;

public class PlogCommand {
    static final String PING = "ping";
    static final String STAT = "stat";
    static final String KILL = "kill";
    static final String ENVI = "envi";

    String getCommand() {
        return command;
    }

    PlogCommand(String command) {
        this.command = command.toLowerCase();
    }

    boolean is(String cmd) {
        return cmd.equals(this.command);
    }

    private final String command;
}
