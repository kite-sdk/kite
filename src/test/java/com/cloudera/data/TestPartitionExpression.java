package com.cloudera.data;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Maps;

public class TestPartitionExpression {

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
      List<Object> partitionKeys = expression.evaluate(vars);

      Assert.assertNotNull(partitionKeys);
      Assert.assertEquals(2, partitionKeys.size());
      Assert.assertEquals("2013", partitionKeys.get(0));
      Assert.assertEquals(i % 7, partitionKeys.get(1));
    }
  }

}
