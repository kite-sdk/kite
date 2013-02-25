package com.cloudera.data.partition;

import org.junit.Test;

import static org.junit.Assert.*;
public class TestPartition {

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

  @Test
  public void test() throws Exception {
    // TODO: this would be expressed in JEXL notation so it can be serialized as a string
    Partition p = new IdentityPartition("month", 12, new HashPartition("userId", 7));

    assertEquals("month", p.getName());
    assertEquals(12, p.getCardinality());

    assertEquals("userId", p.getSubpartition().getName());
    assertEquals(7, p.getSubpartition().getCardinality());

    Entity e = new Entity();
    e.setMonth(2);
    e.setUserId(10);

    assertEquals(12 * 7, p.getAggregateCardinality()); // useful for writers
    assertEquals("month=2/userId=3", p.getPath(e)); // used to find a partition for writers

  }
}
