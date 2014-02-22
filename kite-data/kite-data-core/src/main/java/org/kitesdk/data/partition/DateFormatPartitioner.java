/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.partition;

import java.util.TimeZone;

/**
 * A FieldPartitioner that formats a timestamp (long) in milliseconds since
 *  epoch, such as those returned by {@link System#currentTimeMillis()}, using
 * {@link java.text.SimpleDateFormat}.
 *
 * @since 0.9.0
 * @deprecated will be removed in 0.13.0; moved to spi.partition package
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
           "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
           "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification="Replaced by parent class")
public class DateFormatPartitioner extends
    org.kitesdk.data.spi.partition.DateFormatPartitioner {

  /**
   * Construct a new {@link org.kitesdk.data.partition.DateFormatPartitioner} for Universal Coordinated
   * Time, UTC (+00:00), and cardinality 1095 (3 years, 1 day = 1 partition).
   * @param sourceName Source field name (the field should be a long)
   * @param name Partition name
   * @param format A String format for the {@link java.text.SimpleDateFormat} constructor
   * @deprecated will be removed in 0.13.0; moved to spi.partition package
   */
  @Deprecated
  public DateFormatPartitioner(String sourceName, String name, String format) {
    super(sourceName, name, format);
  }

  /**
   * Construct a new {@link org.kitesdk.data.partition.DateFormatPartitioner} for Universal Coordinated
   * Time, UTC (+00:00).
   * @param sourceName Source field name (the field should be a long)
   * @param name Partition name
   * @param format A String format for the {@link java.text.SimpleDateFormat} constructor
   * @param cardinality
   *          A cardinality hint for the number of partitions that will be
   *          created by this partitioner. For example, "MM-dd" produces about
   *          365 partitions per year.
   * @deprecated will be removed in 0.13.0; moved to spi.partition package
   */
  @Deprecated
  public DateFormatPartitioner(String sourceName, String name, String format, int cardinality, TimeZone zone) {
    super(sourceName, name, format, cardinality, zone);
  }

  @Override
  @Deprecated
  public String valueFromString(String stringValue) {
    return stringValue;
  }
}
