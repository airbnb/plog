package com.airbnb.plog.client;

import java.util.List;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Created by rong_hu on 2/1/17.
 */
public class PlogClientTest {

  @Test
  public void testChunkMessage_NoSplit() {
    int chunkSize = 8;
    byte[] messageBytes = "cafebabe".getBytes();
    List<byte[]> chunks = PlogClient.chunkMessage(messageBytes, chunkSize);

    assertThat(chunks.size(), is(1));
    assertThat(chunks.get(0), equalTo(messageBytes));
  }

  @Test
  public void testChunkMessage_UnevenSplit() {
    int chunkSize = 20;
    byte[] messageBytes = "Brevity is the soul of wit".getBytes();
    List<byte[]> chunks = PlogClient.chunkMessage(messageBytes, chunkSize);

    assertThat(chunks.size(), is(2));
    assertThat(chunks.get(0), equalTo("Brevity is the soul ".getBytes()));
    assertThat(chunks.get(1), equalTo("of wit".getBytes()));
  }
}