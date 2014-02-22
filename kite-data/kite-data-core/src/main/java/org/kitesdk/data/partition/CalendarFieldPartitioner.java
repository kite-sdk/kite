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

import javax.annotation.concurrent.Immutable;

/**
 * A {@link org.kitesdk.data.spi.FieldPartitioner} that extracts the value of a {@link java.util.Calendar} field,
 * such as {@link java.util.Calendar#YEAR}. The UTC timezone is assumed.
 * See subclasses for convenience classes,
 * e.g. {@link org.kitesdk.data.partition.YearFieldPartitioner}. Note that we don't use
 * {@link java.text.SimpleDateFormat} patterns since we want to keep the type information
 * (values are ints).
 * @deprecated will be removed in 0.13.0; moved to spi.partition package
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
           "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
           "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification="Replaced by parent class")
@Immutable
public class CalendarFieldPartitioner extends
    org.kitesdk.data.spi.partition.CalendarFieldPartitioner {

  /**
   * @deprecated will be removed in 0.13.0; moved to spi.partition package
   */
  @Deprecated
  public CalendarFieldPartitioner(String sourceName, String name,
      int calendarField, int cardinality) {
    super(sourceName, name, calendarField, cardinality);
  }

  @Override
  @Deprecated
  public Integer valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }
}
