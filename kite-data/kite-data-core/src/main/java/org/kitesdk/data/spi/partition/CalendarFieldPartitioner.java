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
package org.kitesdk.data.spi.partition;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import java.util.Calendar;
import java.util.TimeZone;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;

/**
 * A {@link FieldPartitioner} that extracts the value of a {@link Calendar} field,
 * such as {@link Calendar#YEAR}. The UTC timezone is assumed.
 * See subclasses for convenience classes,
 * e.g. {@link YearFieldPartitioner}. Note that we don't use
 * {@link java.text.SimpleDateFormat} patterns since we want to keep the type information
 * (values are ints).
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
        "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
@Immutable
public class CalendarFieldPartitioner extends FieldPartitioner<Long, Integer> {

  protected static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  protected int calendarField;

  public CalendarFieldPartitioner(String sourceName, String name,
      int calendarField, int cardinality) {
    super(sourceName, name, Long.class, Integer.class, cardinality);
    this.calendarField = calendarField;
  }

  @Override
  public Integer apply(@Nonnull Long timestamp) {
    Calendar cal = Calendar.getInstance(UTC);
    cal.setTimeInMillis(timestamp);
    return cal.get(calendarField);
  }

  @Override
  public Predicate<Integer> project(Predicate<Long> predicate) {
    return null;
  }

  @Override
  public Predicate<Integer> projectStrict(Predicate<Long> predicate) {
    return null;
  }

  public int getCalendarField() {
    return calendarField;
  }

  @Override
  @Deprecated
  public Integer valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    CalendarFieldPartitioner that = (CalendarFieldPartitioner) o;
    return Objects.equal(this.getSourceName(), that.getSourceName()) &&
        Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.getCardinality(), that.getCardinality());
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getSourceName(), getName(), getCardinality());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("sourceName", getSourceName())
        .add("name", getName())
        .add("cardinality", getCardinality()).toString();
  }
}
