package com.cloudera.data.partition;

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
    // TODO: this would be expressed in JEXL notation so it can be serialized as
    // a string
    final PartitionStrategy p = new IdentityPartitionStrategy("month", 12,
        new HashPartitionStrategy("userId", 7));

    assertEquals("month", p.getName());
    assertEquals(12, p.getCardinality());

    assertEquals("userId", p.getPartition().getName());
    assertEquals(7, p.getPartition().getCardinality());

    Entity e = new Entity();
    e.setMonth(2);
    e.setUserId(10);

    logger.debug("partitionStrategy:{}", p);

    logger
        .debug("path:{}", Lists.transform(Lists.newArrayList(e, 2, "foo"), p));

    assertEquals(12 * 7, p.getAggregateCardinality()); // useful for writers
  }

}
