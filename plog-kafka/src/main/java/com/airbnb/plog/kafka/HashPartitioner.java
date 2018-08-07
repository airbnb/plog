package com.airbnb.plog.kafka;

import java.util.Arrays;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class HashPartitioner implements Partitioner {
  public HashPartitioner (VerifiableProperties props) {
  }

  public int partition(Object key, int numPartitions) {
    if (key instanceof byte[]) {
      // https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/producer/DefaultPartitioner.scala#L27
      // The DefaultPartitioner in kafka 0.8.2 directly use key.hashcode() to calculate the hash.
      // The default .hashcode() implementation of byte[] depends on memory address, which is not
      // what we want. We want to calculate the hash based on the content of the byte array.
      return Math.abs(Arrays.hashCode((byte[])key)) % numPartitions;
    } else {
      return Math.abs(key.hashCode()) % numPartitions;
    }
  }
}
