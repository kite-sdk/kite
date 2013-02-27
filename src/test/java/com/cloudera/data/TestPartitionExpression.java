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
    String expr = "hash(\"username\", 2)";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    Assert.assertEquals(HashPartitionStrategy.class, strategy.getClass());
    Assert.assertEquals("username", strategy.getName());
    Assert.assertEquals(2, strategy.getCardinality());
    Assert.assertNull(strategy.getPartitionStrategy());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

  @Test
  public void testSubpartitionStrategy() {
    String expr = "[hash(\"username\", 2), hash(\"username2\", 3)]";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    Assert.assertEquals(HashPartitionStrategy.class, strategy.getClass());
    Assert.assertEquals("username", strategy.getName());
    Assert.assertEquals(2, strategy.getCardinality());
    Assert.assertNotNull(strategy.getPartitionStrategy());

    PartitionStrategy substrategy = strategy.getPartitionStrategy();
    Assert.assertEquals(HashPartitionStrategy.class, substrategy.getClass());
    Assert.assertEquals("username2", substrategy.getName());
    Assert.assertEquals(3, substrategy.getCardinality());
    Assert.assertNull(substrategy.getPartitionStrategy());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

}
