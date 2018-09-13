package com.airbnb.plog.kafka.partitioner;

import java.util.Base64;
import java.util.Random;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class FlinkPartitionerTest {

  @Test
  public void computePartition() {
    Random random = new Random(42L);
    byte[] id = new byte[16];
    int maxParallelism = 10393;
    int numPartitions = 1983;
    for (int i = 0; i < 40; i++) {
      random.nextBytes(id);
      String encoded = Base64.getEncoder().encodeToString(id);
      int testPartition = FlinkPartitioner.computePartition(encoded, numPartitions, maxParallelism);
      int flinkPartition = KeyGroupRangeAssignment.assignKeyToParallelOperator(encoded, maxParallelism, numPartitions);

      assertThat(testPartition, equalTo(flinkPartition));
    }
  }
}