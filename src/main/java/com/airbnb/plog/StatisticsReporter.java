package com.airbnb.plog;

public interface StatisticsReporter {
    long receivedTcpMessage();
    long receivedUdpSimpleMessage();
    long receivedUdpInvalidVersion();
    long receivedV0InvalidType();
    long receivedV0Command();
    long receivedUnknownCommand();
    long receivedV0MultipartMessage();
    long receivedV0MultipartFragment(int index);
    long failedToSend();
    long exception();
}
