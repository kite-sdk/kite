/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import org.junit.Ignore;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.partition.HashFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionExpression {

  @Test
  public void testPartitionStrategy() {
    String expr = "hash(\"username\", \"username_part\", 2)";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    List<FieldPartitioner> fieldPartitioners =
        Accessor.getDefault().getFieldPartitioners(strategy);
    Assert.assertEquals(1, fieldPartitioners.size());
    FieldPartitioner fp = fieldPartitioners.get(0);
    Assert.assertEquals(HashFieldPartitioner.class, fp.getClass());
    Assert.assertEquals("username", fp.getSourceName());
    Assert.assertEquals("username_part", fp.getName());
    Assert.assertEquals(2, fp.getCardinality());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

  @Test
  public void testSubpartitionStrategy() {
    String expr = "[hash(\"username\", \"username_part\", 2), hash(\"username2\", " +
        "\"username2_part\", 3)]";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    List<FieldPartitioner> fieldPartitioners =
        Accessor.getDefault().getFieldPartitioners(strategy);
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    Assert.assertEquals(HashFieldPartitioner.class, fp0.getClass());
    Assert.assertEquals("username_part", fp0.getName());
    Assert.assertEquals(2, fp0.getCardinality());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    Assert.assertEquals(HashFieldPartitioner.class, fp1.getClass());
    Assert.assertEquals("username2_part", fp1.getName());
    Assert.assertEquals(3, fp1.getCardinality());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

  @Test
  public void testMixedSubpartitionStrategy() {
    String expr = "[year(\"timestamp\", \"year\"), minute(\"timestamp\", \"minute\")]";
    PartitionExpression expression = new PartitionExpression(expr, true);

    PartitionStrategy strategy = expression.evaluate();
    List<FieldPartitioner> fieldPartitioners =
        Accessor.getDefault().getFieldPartitioners(strategy);
    Assert.assertEquals(2, fieldPartitioners.size());

    FieldPartitioner fp0 = fieldPartitioners.get(0);
    Assert.assertEquals(YearFieldPartitioner.class, fp0.getClass());
    Assert.assertEquals("timestamp", fp0.getSourceName());
    Assert.assertEquals("year", fp0.getName());

    FieldPartitioner fp1 = fieldPartitioners.get(1);
    Assert.assertEquals(MinuteFieldPartitioner.class, fp1.getClass());
    Assert.assertEquals("timestamp", fp1.getSourceName());
    Assert.assertEquals("minute", fp1.getName());

    Assert.assertEquals(expr, PartitionExpression.toExpression(strategy));
  }

  @Test
  public void testRange() {
    PartitionStrategy rangeStrategy = new PartitionStrategy.Builder()
        .range("color", "blue", "green", "orange", "red", "white")
        .build();
    PartitionStrategy copy = new PartitionExpression(
        PartitionExpression.toExpression(rangeStrategy), true).evaluate();
    Assert.assertEquals(rangeStrategy, copy);
  }

  @Test
  public void testIntRange() {
    PartitionStrategy intRangeStrategy = new PartitionStrategy.Builder()
        .range("intField", 5, 10, 15, 20, 25)
        .build();
    PartitionStrategy copy = new PartitionExpression(
        PartitionExpression.toExpression(intRangeStrategy), true).evaluate();
    Assert.assertEquals(intRangeStrategy, copy);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testIntIdentity() {
    PartitionStrategy intIdStrategy = new PartitionStrategy.Builder()
        .identity("intField")
        .build();
    PartitionStrategy copy = new PartitionExpression(
        PartitionExpression.toExpression(intIdStrategy), true).evaluate();
    Assert.assertEquals(intIdStrategy, copy);
  }

  @Test
  @Ignore
  public void testList() {
    PartitionStrategy listStrategy = new PartitionStrategy.Builder()
        // TODO: can't use a LitFieldPartitioner yet
        .build();
    PartitionStrategy copy = new PartitionExpression(
        PartitionExpression.toExpression(listStrategy), true).evaluate();
    Assert.assertEquals(listStrategy, copy);
  }
}
