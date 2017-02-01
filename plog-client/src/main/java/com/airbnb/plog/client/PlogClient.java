package com.airbnb.plog.client;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;

/**
 * ## Plog client for Java
 * Example:
 *
 * ```java
 * PlogClient plogClient = new PlogClient("127.0.0.1", 23456, 64000);
 * plogClient.send("My hovercraft is full of eels.");
 * ```
 * You can configure the client at initialization by passing these options:
 *
 * + host - The host of the Plog process (e.g., 'localhost')
 * + port - The port on which Plog is listening (e.g., 23456)
 * + chunkSize - The maximum payload size for multipart datagrams (e.g., 64,000)
 */
@Slf4j
public class PlogClient implements Closeable {

  public static final int DEFAULT_CHUNK_SIZE = 64000;

  private AtomicInteger lastMessageId;

  private final InetAddress address;

  private final int port;

  private final int chunkSize;

  private DatagramSocket socket;

  public PlogClient(String host, int port) {
    this(host, port, DEFAULT_CHUNK_SIZE);
  }

  public PlogClient(String host, int port, int chunkSize) {
    Preconditions.checkNotNull(host, "host cannot be null!");
    Preconditions.checkArgument(port > 1024 && port < 65536, "Must provide a valid port number!");
    Preconditions.checkArgument(chunkSize < 65483, "Maximum Plog UDP data length is 65483 bytes!");

    openSocket();
    try {
      address = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      log.error("Unknown address {}", host, e);
      throw Throwables.propagate(e);
    }
    this.port = port;
    this.chunkSize = chunkSize;
    this.lastMessageId = new AtomicInteger(1);
  }

  /**
   * Send the message to Plog server.
   */
  public void send(String message) {
    // ISO-8859-1 is ASCII-8bit. It's equivalent to ruby's BINARY encoding.
    byte[] messageBytes = message.getBytes(Charset.forName("ISO-8859-1"));
    int messageId = lastMessageId.getAndIncrement();
    lastMessageId.compareAndSet(Integer.MAX_VALUE, 1);
    int messageLength = messageBytes.length;
    byte[] checksum = computeChecksum(messageBytes);
    List<byte[]> chunks = chunkMessage(messageBytes, chunkSize);
    int count = chunks.size();

    log.debug("Plog: sending {}; {} chunk(s)", messageId, count);
    for (int i = 0; i < count; i++) {
      sendToSocket(MultipartMessage.encode(messageId,
          messageLength,
          checksum,
          chunkSize,
          count,
          i,
          chunks.get(i)));
    }
  }

  @VisibleForTesting
  static byte[] computeChecksum(byte[] messageBytes) {
    // Checksum in little-endian.
    byte[] checksum = Hashing.murmur3_32().hashBytes(messageBytes).asBytes();
    // Reverse checksum bytes to make it big-endian.
    byte temp;
    int start = 0, end = 3;
    while (start < end) {
      temp = checksum[start];
      checksum[start] = checksum[end];
      checksum[end] = temp;
      start++;
      end--;
    }
    return checksum;
  }

  @VisibleForTesting
  static List<byte[]> chunkMessage(byte[] messageBytes, int size) {
    final List<byte[]> chunks = new ArrayList<byte[]>();
    int startIndex = 0;
    while (startIndex + size < messageBytes.length) {
      chunks.add(Arrays.copyOfRange(messageBytes, startIndex, startIndex + size));
      startIndex += size;
    }
    // If there's some remaining bytes,
    // copy them up to the end of messageBytes.
    if (startIndex < messageBytes.length) {
      chunks.add(Arrays.copyOfRange(messageBytes, startIndex, messageBytes.length));
    }
    return chunks;
  }

  private void openSocket() {
    try {
      socket = new DatagramSocket();
    } catch (SocketException e) {
      log.error("Cannot open socket", e);
      throw Throwables.propagate(e);
    }
  }

  private void sendToSocket(byte[] chunk) {
    DatagramPacket packet = new DatagramPacket(chunk, chunk.length, address, port);
    try {
      log.trace("Sending {} to UDP port {}", chunk, port);
      socket.send(packet);
    } catch (IOException e) {
      log.error("Error sending packet!", e);
      socket.close();
      openSocket();
    }
  }

  @Override
  public void close() throws IOException {
    if (socket == null) return;
    socket.close();
  }
}