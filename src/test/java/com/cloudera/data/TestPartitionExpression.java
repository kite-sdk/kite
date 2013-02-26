package com.cloudera.data;

import com.cloudera.data.partition.HashPartitionStrategy;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPartitionExpression {

  private static final Logger logger = LoggerFactory
      .getLogger(TestPartitionExpression.class);

  @Test
  public void testPartitionStrategy() {
    PartitionExpression expression = new PartitionExpression(
        "new(\"com.cloudera.data.partition.HashPartitionStrategy\", \"username\", 2)",
        true);

    PartitionStrategy strategy = expression.evaluate();
    Assert.assertEquals(HashPartitionStrategy.class, strategy.getClass());
    Assert.assertEquals("username", strategy.getName());
    Assert.assertEquals(2, strategy.getCardinality());
    Assert.assertNull(strategy.getPartition());
  }

  @Test
  public void testSubpartitionStrategy() {
    PartitionExpression expression = new PartitionExpression(
        "new(\"com.cloudera.data.partition.HashPartitionStrategy\", \"username\", 2, " +
            "new(\"com.cloudera.data.partition.HashPartitionStrategy\", \"username2\", 3))",
        true);

    PartitionStrategy strategy = expression.evaluate();
    Assert.assertEquals(HashPartitionStrategy.class, strategy.getClass());
    Assert.assertEquals("username", strategy.getName());
    Assert.assertEquals(2, strategy.getCardinality());
    Assert.assertNotNull(strategy.getPartition());

    PartitionStrategy substrategy = strategy.getPartition();
    Assert.assertEquals(HashPartitionStrategy.class, substrategy.getClass());
    Assert.assertEquals("username2", substrategy.getName());
    Assert.assertEquals(3, substrategy.getCardinality());
    Assert.assertNull(substrategy.getPartition());
  }

}
