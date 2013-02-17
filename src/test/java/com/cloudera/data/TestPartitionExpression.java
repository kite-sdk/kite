package com.cloudera.data;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TestPartitionExpression {

  private static final Logger logger = LoggerFactory
      .getLogger(TestPartitionExpression.class);

  @Test
  public void testSingleField() {
    PartitionExpression expression = new PartitionExpression("record.year");

    Map<String, Object> vars = Maps.newHashMap();
    vars.put("year", "2013");

    expression.evaluate(vars);
  }

  @Test
  public void testMultiField() {
    PartitionExpression expression = new PartitionExpression(
        "[record.year, record.month]");

    Map<String, Object> vars = Maps.newHashMap();
    vars.put("year", "2013");
    vars.put("month", "01");

    expression.evaluate(vars);
  }

  @Test
  public void testOptionalField() {
    PartitionExpression expression = new PartitionExpression(
        "[record.year, record.month ?: \"00\"]");

    Map<String, Object> vars = Maps.newHashMap();
    vars.put("year", "2013");

    expression.evaluate(vars);

    vars.put("month", "01");

    expression.evaluate(vars);
  }

  @Test
  public void testHashedField() {
    PartitionExpression expression = new PartitionExpression(
        "[record.year, record.user_id % 7]");

    Map<String, Object> vars = Maps.newHashMap();
    vars.put("year", "2013");

    for (int i = 1; i <= 30; i++) {
      vars.put("user_id", i);
      String partitionName = expression.evaluate(vars);

      logger.debug("partition name:{}", partitionName);

      Assert.assertNotNull(partitionName);
      Assert.assertEquals("2013/" + i % 7, partitionName);
    }
  }

}
