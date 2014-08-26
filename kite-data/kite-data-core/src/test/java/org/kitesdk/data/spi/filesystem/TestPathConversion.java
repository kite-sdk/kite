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
package org.kitesdk.data.spi.filesystem;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.partition.YearFieldPartitioner;
import org.kitesdk.data.spi.StorageKey;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.List;

public class TestPathConversion {

  private static final Schema schema = SchemaBuilder.record("Event").fields()
      .requiredLong("id")
      .requiredLong("timestamp")
      .endRecord();

  private static final PathConversion convert = new PathConversion(schema);
  private static final Splitter EQ = Splitter.on('=');

  @Test
  public void testDirnameMinWidth() {
    Assert.assertEquals("min=01",
        convert.dirnameForValue(
            new MinuteFieldPartitioner("timestamp", "min"), 1));
    Assert.assertEquals("hour=01",
        convert.dirnameForValue(
            new HourFieldPartitioner("timestamp", "hour"), 1));
    Assert.assertEquals("day=01",
        convert.dirnameForValue(
            new DayOfMonthFieldPartitioner("timestamp", "day"), 1));
    Assert.assertEquals("month=01",
        convert.dirnameForValue(
            new MonthFieldPartitioner("timestamp", "month"), 1));
    Assert.assertEquals("year=2013",
        convert.dirnameForValue(
            new YearFieldPartitioner("timestamp", "year"), 2013));
  }

  @Test
  public void testUsesFieldName() {
    Assert.assertEquals("day_of_month_field",
        Iterables.getFirst(EQ.split(convert.dirnameForValue(
            new DayOfMonthFieldPartitioner("day", "day_of_month_field"), 10)),
            null));
  }

  @Test
  public void testIgnoresPartitionName() {
    Assert.assertEquals("10", convert.dirnameToValueString("10"));
    Assert.assertEquals("10", convert.dirnameToValueString("=10"));
    Assert.assertEquals("10", convert.dirnameToValueString("anything=10"));
    Assert.assertEquals(10, (int) convert.valueForDirname(
        new MonthFieldPartitioner("timestamp", "month"), "10"));
    Assert.assertEquals(10, (int) convert.valueForDirname(
        new MonthFieldPartitioner("timestamp", "month"), "=10"));
    Assert.assertEquals(10, (int) convert.valueForDirname(
        new MonthFieldPartitioner("timestamp", "month"), "anything=10"));
    Assert.assertEquals(10, (int) convert.valueForDirname(
        new MonthFieldPartitioner("timestamp", "month"), "even=strange=10"));
  }

  @Test
  public void testNoValidation() {
    Assert.assertEquals(13, (int) convert.valueForDirname(
        new MonthFieldPartitioner("timestamp", "month"), "month=13"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFromKey() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();

    StorageKey key = new StorageKey(strategy);
    key.replaceValues((List) Lists.newArrayList(2013, 11, 5));

    Assert.assertEquals(
        new Path("year=2013/month=11/day=05"),
        convert.fromKey(key));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToKey() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();

    StorageKey expected = new StorageKey(strategy);
    expected.replaceValues((List) Lists.newArrayList(2013, 11, 5));

    Assert.assertEquals(expected, convert.toKey(
        new Path("year=2013/month=11/day=5"), new StorageKey(strategy)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void toDirNameIdentityWithSlashes() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .identity("name")
        .identity("address")
        .build();

    StorageKey key = new StorageKey(strategy);
    key.replaceValues((List) Lists.newArrayList("John Doe", "NY/USA"));

    Assert.assertEquals(
        new Path("name_copy=John+Doe/address_copy=NY%2FUSA"),
        convert.fromKey(key));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void toDirNameIdentityWithNonString() {
    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .identity("id")
        .build();

    StorageKey expected = new StorageKey(strategy);
    expected.replace(0, 0L);

    Assert.assertEquals("Should convert to schema type",
        expected,
        convert.toKey(new Path("id=0"), new StorageKey(strategy)));
  }
}
