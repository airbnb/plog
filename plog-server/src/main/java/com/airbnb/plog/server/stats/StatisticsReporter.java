package com.airbnb.plog.server.stats;

public interface StatisticsReporter {
    long receivedUdpSimpleMessage();

    long receivedUdpInvalidVersion();

    long receivedV0InvalidType();

    long receivedV0Command();

    long receivedUnknownCommand();

    long receivedV0InvalidMultipartHeader();

    long receivedV0MultipartMessage();

    long exception();

    long receivedV0MultipartFragment(int index);

    long receivedV0InvalidChecksum(int index);

    long foundHolesFromDeadPort(int holesFound);

    long foundHolesFromNewMessage(int holesFound);

    long receivedV0InvalidMultipartFragment(final int fragmentIndex, final int expectedFragments);

    long missingFragmentInDroppedMessage(final int fragmentIndex, final int expectedFragments);

    long unhandledObject();
}
