package com.airbnb.plog.client;

import java.nio.charset.Charset;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class MultipartMessageTest {

  @Test
  public void testEncode() {
    byte[] message = "abc".getBytes(Charset.forName("ISO-8859-1"));
    byte[] checksum = PlogClient.computeChecksum(message);
    byte[] chunk = MultipartMessage.encode(1, 3, checksum, 64000, 1, 0, message);
    byte[] expected = {0, 1, 0, 1, 0, 0, (byte)0xfa, 0, 0, 0, 0, 1, 0, 0, 0, 3,
        (byte)0xb3, (byte)0xdd, (byte)0x93, (byte)0xfa, 0, 0, 0, 0,
        (byte)0x61, (byte)0x62, (byte)0x63};
    assertArrayEquals(expected, chunk);
  }
}