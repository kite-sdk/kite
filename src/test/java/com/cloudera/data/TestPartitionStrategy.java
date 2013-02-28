package com.cloudera.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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

    assertEquals("month", p.getName());
    assertEquals(12, p.getCardinality());

    assertEquals("userId", p.getPartitionStrategy().getName());
    assertEquals(7, p.getPartitionStrategy().getCardinality());

    Entity e = new Entity();
    e.setMonth(2);
    e.setUserId(10);

    logger.debug("partitionStrategy:{}", p);

    logger
        .debug("path:{}", Lists.transform(Lists.newArrayList(e, 2, "foo"), p));

    assertEquals(12 * 7, p.getAggregateCardinality()); // useful for writers
  }

}
