/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import org.apache.avro.Schema;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.SchemaUtil;
import org.kitesdk.data.spi.partition.DayOfMonthFieldPartitioner;
import org.kitesdk.data.spi.partition.HourFieldPartitioner;
import org.kitesdk.data.spi.partition.MinuteFieldPartitioner;
import org.kitesdk.data.spi.partition.MonthFieldPartitioner;
import org.kitesdk.data.spi.Conversions;
import org.kitesdk.data.spi.StorageKey;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

public class PathConversion {

  private final Schema schema;

  public PathConversion(Schema schema) {
    this.schema = schema;
  }

  public StorageKey toKey(Path fromPath, StorageKey storage) {
    final List<FieldPartitioner> partitioners =
        storage.getPartitionStrategy().getFieldPartitioners();
    final List<Object> values = Lists.newArrayList(
        new Object[partitioners.size()]);

    Path currentPath = fromPath;
    int index = partitioners.size() - 1;
    while (currentPath != null && index >= 0) {

      values.set(index, valueForDirname(
          (FieldPartitioner<?, ?>) partitioners.get(index),
          currentPath.getName()));

      // update
      currentPath = currentPath.getParent();
      index -= 1;
    }

    storage.replaceValues(values);
    return storage;
  }

  public Path fromKey(StorageKey key) {
    final StringBuilder pathBuilder = new StringBuilder();
    final List<FieldPartitioner> partitioners =
        key.getPartitionStrategy().getFieldPartitioners();

    for (int i = 0; i < partitioners.size(); i++) {
      final FieldPartitioner fp = partitioners.get(i);
      if (i != 0) {
        pathBuilder.append(Path.SEPARATOR_CHAR);
      }
      @SuppressWarnings("unchecked")
      String dirname = dirnameForValue(fp, key.get(i));
      pathBuilder.append(dirname);
    }

    return new Path(pathBuilder.toString());
  }

  private static final Splitter PART_SEP = Splitter.on('=');
  private static final Joiner PART_JOIN = Joiner.on('=');
  private static final Map<Class<?>, Integer> WIDTHS =
      ImmutableMap.<Class<?>, Integer>builder()
          .put(MinuteFieldPartitioner.class, 2)
          .put(HourFieldPartitioner.class, 2)
          .put(DayOfMonthFieldPartitioner.class, 2)
          .put(MonthFieldPartitioner.class, 2)
          .build();

  public static <T> String dirnameForValue(FieldPartitioner<?, T> field, T value) {
    try{
      return PART_JOIN.join(field.getName(),
          URLEncoder.encode(Conversions.makeString(value, WIDTHS.get(field.getClass())), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("Unable to find UTF-8 encoding.");
    }
  }

  public <T> T valueForDirname(FieldPartitioner<?, T> field, String name) {
    // this could check that the field name matches the directory name
    return Conversions.convert(valueStringForDirname(name),
        SchemaUtil.getPartitionType(field, schema));
  }

  public String valueStringForDirname(String name) {
    try {
      return URLDecoder.decode(Iterables.getLast(PART_SEP.split(name)), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("Unable to find UTF-8 encoding.");
    }
  }
}
