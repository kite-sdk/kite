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
package org.kitesdk.data.partition;

import org.kitesdk.data.spi.FieldPartitioner;

/**
 * Convenience class so you can say, for example, <code>hash("username", 2)</code> in
 * JEXL.
 * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
 */
@Deprecated
@SuppressWarnings("unchecked")
public class PartitionFunctions {

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Object, Integer> hash(String name, int buckets) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.hash(name, buckets);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Object, Integer> hash(String sourceName, String name, int buckets) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.hash(sourceName, name, buckets);
  }

  /**
   * @deprecated will be removed in 0.13.0; use
   * {@link org.kitesdk.data.spi.partition.PartitionFunctions#identity(String, Class, int)}.
   */
  @Deprecated
  public static FieldPartitioner identity(String name, int buckets) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.identity(name, String.class, buckets);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static <S> FieldPartitioner<S, S> identity(String name, Class<S> type,
      int buckets) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.identity(name, type, buckets);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Integer, Integer> range(String name, int... upperBounds) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.range(name, upperBounds);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<String, String> range(String name, String... upperBounds) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.range(name, upperBounds);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, Integer> year(String sourceName, String name) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.year(sourceName, name);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, Integer> month(String sourceName, String name) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.month(sourceName, name);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, Integer> day(String sourceName, String name) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.day(sourceName, name);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, Integer> hour(String sourceName, String name) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.hour(sourceName, name);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, Integer> minute(String sourceName, String name) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.minute(sourceName, name);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static FieldPartitioner<Long, String> dateFormat(String sourceName, String name, String format) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.dateFormat(sourceName, name, format);
  }

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public static String toExpression(FieldPartitioner fieldPartitioner) {
    return org.kitesdk.data.spi.partition.PartitionFunctions.toExpression(fieldPartitioner);
  }
}
