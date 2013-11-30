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
package com.cloudera.cdk.data.partition;

import com.cloudera.cdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;

/**
 * Convenience class so you can say, for example, <code>hash("username", 2)</code> in
 * JEXL.
 */
@Beta
@SuppressWarnings("unchecked")
public class PartitionFunctions {

  public static FieldPartitioner<Object, Integer> hash(String name, int buckets) {
    return new HashFieldPartitioner(name, buckets);
  }

  public static FieldPartitioner<Object, Integer> hash(String sourceName, String name, int buckets) {
    return new HashFieldPartitioner(sourceName, name, buckets);
  }

  /**
   * @deprecated Use {@link #identity(String, Class, int)}.
   */
  @Deprecated
  public static FieldPartitioner identity(String name, int buckets) {
    return new IdentityFieldPartitioner(name, String.class, buckets);
  }

  public static <S> FieldPartitioner<S, S> identity(String name, Class<S> type,
      int buckets) {
    return new IdentityFieldPartitioner(name, type, buckets);
  }

  @Beta
  public static FieldPartitioner<Integer, Integer> range(String name, int... upperBounds) {
    return new IntRangeFieldPartitioner(name, upperBounds);
  }

  @Beta
  public static FieldPartitioner<String, String> range(String name, String... upperBounds) {
    return new RangeFieldPartitioner(name, upperBounds);
  }

  @Beta
  public static FieldPartitioner<Long, Integer> year(String sourceName, String name) {
    return new YearFieldPartitioner(sourceName, name);
  }

  @Beta
  public static FieldPartitioner<Long, Integer> month(String sourceName, String name) {
    return new MonthFieldPartitioner(sourceName, name);
  }

  @Beta
  public static FieldPartitioner<Long, Integer> day(String sourceName, String name) {
    return new DayOfMonthFieldPartitioner(sourceName, name);
  }

  @Beta
  public static FieldPartitioner<Long, Integer> hour(String sourceName, String name) {
    return new HourFieldPartitioner(sourceName, name);
  }

  @Beta
  public static FieldPartitioner<Long, Integer> minute(String sourceName, String name) {
    return new MinuteFieldPartitioner(sourceName, name);
  }

  @Beta
  public static FieldPartitioner<Long, String> dateFormat(String sourceName, String name, String format) {
    return new DateFormatPartitioner(sourceName, name, format);
  }

}
