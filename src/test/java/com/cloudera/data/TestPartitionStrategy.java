package com.cloudera.data;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.List;

public class TestPartitionStrategy {

  static class Entity {
    int month;
    int userId;

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    public int getUserId() {
      return userId;
    }

    public void setUserId(int userId) {
      this.userId = userId;
    }
  }

  private static final Logger logger = LoggerFactory
      .getLogger(TestPartitionStrategy.class);

  @Test
  public void test() throws Exception {
    final PartitionStrategy p = new PartitionStrategy.Builder()
        .identity("month", 12)
        .hash("userId", 7)
        .get();

    List<FieldPartitioner> fieldPartitioners = p.getFieldPartitioners();
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    assertEquals("month", fp0.getName());
    assertEquals(12, fp0.getCardinality());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    assertEquals("userId", fp1.getName());
    assertEquals(7, fp1.getCardinality());

    Entity e = new Entity();
    e.setMonth(2);
    e.setUserId(10);

    logger.debug("partitionStrategy:{}", p);

    assertEquals(12 * 7, p.getCardinality()); // useful for writers
  }

}
