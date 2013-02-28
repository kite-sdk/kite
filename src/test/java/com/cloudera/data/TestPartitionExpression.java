package com.cloudera.data;

import com.cloudera.data.partition.HashFieldPartitioner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestPartitionExpression {

  private static final Logger logger = LoggerFactory
      .getLogger(TestPartitionExpression.class);

  @Test
  public void testPartitionStrategy() {
    String expr = "hash(\"username\", 2)";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    List<FieldPartitioner> fieldPartitioners = strategy.getFieldPartitioners();
    Assert.assertEquals(1, fieldPartitioners.size());
    FieldPartitioner fp = fieldPartitioners.get(0);
    Assert.assertEquals(HashFieldPartitioner.class, fp.getClass());
    Assert.assertEquals("username", fp.getName());
    Assert.assertEquals(2, fp.getCardinality());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

  @Test
  public void testSubpartitionStrategy() {
    String expr = "[hash(\"username\", 2), hash(\"username2\", 3)]";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    List<FieldPartitioner> fieldPartitioners = strategy.getFieldPartitioners();
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    Assert.assertEquals(HashFieldPartitioner.class, fp0.getClass());
    Assert.assertEquals("username", fp0.getName());
    Assert.assertEquals(2, fp0.getCardinality());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    Assert.assertEquals(HashFieldPartitioner.class, fp1.getClass());
    Assert.assertEquals("username2", fp1.getName());
    Assert.assertEquals(3, fp1.getCardinality());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

}
