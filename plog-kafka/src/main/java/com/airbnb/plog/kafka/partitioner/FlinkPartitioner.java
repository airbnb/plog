package com.airbnb.plog.kafka.partitioner;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

@Slf4j
public class FlinkPartitioner implements Partitioner {
  private static final String MAX_PARALLELISM_CONFIG = "partitioner.maxParallelism";
  private final AtomicInteger counter = new AtomicInteger((new Random()).nextInt());
  private final AtomicInteger normalCounter = new AtomicInteger(0);
  private int maxParallelism = 16386;

  private static int toPositive(int number) {
    return number & Integer.MAX_VALUE;
  }

  public void configure(Map<String, ?> configs) {
    Object maxParallelism = configs.get(MAX_PARALLELISM_CONFIG);
    log.warn("Configuration is {}", configs);
    if (maxParallelism instanceof Number) {
      this.maxParallelism = ((Number) maxParallelism).intValue();
    } else if (maxParallelism instanceof String) {
      try {
        this.maxParallelism = Integer.parseInt((String) maxParallelism);
      } catch (NumberFormatException e) {
        log.error("Failed to parse maxParallelism value {}", maxParallelism);
      }
    }
  }


  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    int msgCount = normalCounter.incrementAndGet();
    if (msgCount % 1000 == 0) {
      log.info("Sent {} messages", msgCount);
    }

    if (key == null) {
      int nextValue = this.counter.getAndIncrement();
      List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
      if (availablePartitions.size() > 0) {
        int part = toPositive(nextValue) % availablePartitions.size();
        return availablePartitions.get(part).partition();
      } else {
        return toPositive(nextValue) % numPartitions;
      }
    } else {
      return computePartition(key, numPartitions, maxParallelism);
    }
  }

  public void close() {
  }

  /*
   * These static functions are derived from the code in KeyGroupRangeAssignment.
   * https://github.com/apache/flink/blob/8674b69964eae50cad024f2c5caf92a71bf21a09/flink-runtime/src/main/java/org/apache/flink/runtime/state/KeyGroupRangeAssignment.java
   * The full dependency into this project results in a significant jar size increase.
   *
   * By pulling in only these functions, we keep the distribution size under 10 MB.
   */

  static int computePartition(Object key, int numPartitions, int maxParallelism) {
    int group = murmurHash(key.hashCode()) % maxParallelism;
    return (group * numPartitions) / maxParallelism;
  }

  static int murmurHash(int code) {
    code *= 0xcc9e2d51;
    code = Integer.rotateLeft(code, 15);
    code *= 0x1b873593;

    code = Integer.rotateLeft(code, 13);
    code = code * 5 + 0xe6546b64;

    code ^= 4;
    code = bitMix(code);

    if (code >= 0) {
      return code;
    } else if (code != Integer.MIN_VALUE) {
      return -code;
    } else {
      return 0;
    }
  }

  static int bitMix(int in) {
    in ^= in >>> 16;
    in *= 0x85ebca6b;
    in ^= in >>> 13;
    in *= 0xc2b2ae35;
    in ^= in >>> 16;
    return in;
  }
}
