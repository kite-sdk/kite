package com.cloudera.data.partition;

import org.junit.Assert;
import org.junit.Test;

public class TestRangeFieldPartitioner {

  @Test
  public void test() {
    RangeFieldPartitioner partitioner = new RangeFieldPartitioner("username",
        new Comparable<?>[] { "barry", "susan", "zippy" });

    Assert.assertEquals(3, partitioner.getCardinality());
    Assert.assertEquals("barry", partitioner.apply("adam"));
    Assert.assertEquals("barry", partitioner.apply("barry"));
    Assert.assertEquals("susan", partitioner.apply("charlie"));
    Assert.assertEquals("zippy", partitioner.apply("yvette"));

    Exception ex = null;

    try {
      partitioner.apply("zzzzz out of range");
    } catch (Exception e) {
      ex = e;
    }

    Assert.assertNotNull(ex);
    Assert.assertTrue(ex instanceof IllegalArgumentException);
  }

}
