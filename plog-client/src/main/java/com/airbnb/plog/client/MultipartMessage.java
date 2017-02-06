package com.airbnb.plog.client;

import java.nio.ByteBuffer;

/**
 * ## A chunk in [a multi-part UDP message](https://github.com/airbnb/plog#packet-type-01-fragmented-message).
 */
public class MultipartMessage {
  public static final byte PROTOCOL_VERSION = 0;

  public static final byte TYPE_MULTIPART_MESSGAE = 1;

  private static final int NUM_HEADER_BYTES = 24;
  /**
   * Encode the payload as a chunk in a multi-part UDP message.
   *
   * @param messageId
   * @param length
   * @param checksum
   * @param chunkSize
   * @param count
   * @param index
   * @param payload
   * @return the encoded bytes ready for UDP transmission.
   */
  public static byte[] encode(int messageId,
                              int length,
                              byte[] checksum,
                              int chunkSize,
                              int count,
                              int index,
                              byte[] payload) {
    // ByteBuffer by default is big-endian.
    ByteBuffer byteBuffer = ByteBuffer.allocate(NUM_HEADER_BYTES + payload.length);
    // Some temporary byte buffer used.
    ByteBuffer twoBytes = ByteBuffer.allocate(2);
    ByteBuffer fourBytes = ByteBuffer.allocate(4);

    // Byte 00: version (00)
    byteBuffer.put(PROTOCOL_VERSION);
    // Byte 01: Packet type 01: fragmented message
    byteBuffer.put(TYPE_MULTIPART_MESSGAE);
    // Bytes 02-03: Fragment count for the message.
    byteBuffer.put(twoBytes.putShort(0,(short)count).array(), 0, 2);
    // Bytes 04-05: Index of this fragment in the message.
    byteBuffer.put(twoBytes.putShort(0, (short)index).array(), 0, 2);
    // Bytes 06-07: Byte length of the payload for each fragment in the message.
    byteBuffer.put(twoBytes.putShort(0, (short)chunkSize).array(), 0, 2);
    // Bytes 08-11: Second half of the identifier for the message.
    // Messages are identified by the UDP client port and this second half.
    // Needs to increment with each message for hole detection.
    byteBuffer.put(fourBytes.putInt(0, messageId).array(), 0, 4);
    // Bytes 12-15: signed, big-endian, 32-bit integer below 2,147,483,647.
    // Total byte length of the message.
    byteBuffer.put(fourBytes.putInt(0, length).array(), 0, 4);
    // Bytes 16-19: MurmurHash3 hash of the total message payload.
    byteBuffer.put(checksum, 0, 4);
    // Bytes 20-23: null bytes.
    byteBuffer.put(new byte[4], 0, 4);
    // Bytes (24+taglength)-: Bytes. Payload. Will only read the payload length.
    byteBuffer.put(payload, 0, payload.length);
    return byteBuffer.array();
  }
}
