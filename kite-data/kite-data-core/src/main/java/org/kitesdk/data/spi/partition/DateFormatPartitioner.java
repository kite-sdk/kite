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

package org.kitesdk.data.spi.partition;

import com.google.common.base.CharMatcher;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.predicates.Exists;
import org.kitesdk.data.spi.predicates.In;
import org.kitesdk.data.spi.predicates.Predicates;
import org.kitesdk.data.spi.predicates.Range;
import org.kitesdk.data.spi.predicates.Ranges;

/**
 * A FieldPartitioner that formats a timestamp (long) in milliseconds since
 *  epoch, such as those returned by {@link System#currentTimeMillis()}, using
 * {@link SimpleDateFormat}.
 *
 * @since 0.9.0
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value={
    "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
    "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE"},
    justification="False positive due to generics.")
@Immutable
public class DateFormatPartitioner extends FieldPartitioner<Long, String> {

  private static final String DEFAULT_TIME_ZONE = "UTC";
  private final SimpleDateFormat format;

  /**
   * Construct a new {@link DateFormatPartitioner} for Universal Coordinated
   * Time, UTC (+00:00), and cardinality 1095 (3 years, 1 day = 1 partition).
   * @param sourceName Source field name (the field should be a long)
   * @param name Partition name
   * @param format A String format for the {@link SimpleDateFormat} constructor
   */
  public DateFormatPartitioner(String sourceName, String name, String format) {
    this(sourceName, name, format, 1095, TimeZone.getTimeZone(DEFAULT_TIME_ZONE));
  }

  /**
   * Construct a new {@link DateFormatPartitioner} for Universal Coordinated
   * Time, UTC (+00:00).
   * @param sourceName Source field name (the field should be a long)
   * @param name Partition name
   * @param format A String format for the {@link SimpleDateFormat} constructor
   * @param cardinality
   *          A cardinality hint for the number of partitions that will be
   *          created by this partitioner. For example, "MM-dd" produces about
   *          365 partitions per year.
   */
  public DateFormatPartitioner(String sourceName, String name, String format, int cardinality, TimeZone zone) {
    super(sourceName, name, Long.class, String.class, cardinality);
    Preconditions.checkArgument(CharMatcher.is('/').matchesNoneOf(format),
        "Illegal format: \"/\" is not allowed (use multiple partition fields)");
    this.format = new SimpleDateFormat(format);
    this.format.setTimeZone(zone);
  }

  public String getPattern() {
    return format.toPattern();
  }

  @Override
  public String apply(Long value) {
    return format.format(new Date(value));
  }

  @Override
  @Deprecated
  public String valueFromString(String stringValue) {
    return stringValue;
  }

  @Override
  public Predicate<String> project(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else if (predicate instanceof In) {
      return ((In<Long>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      // FIXME: This project is only true in some cases
      // true for yyyy-MM-dd, but not dd-MM-yyyy
      // this is lossy, so the final range must be closed:
      //   (2013-10-4 20:17:55, ...] => [2013-10-4, ...]
      return Ranges.transformClosed((Range<Long>) predicate, this);
    } else {
      return null;
    }
  }

  @Override
  public Predicate<String> projectStrict(Predicate<Long> predicate) {
    if (predicate instanceof Exists) {
      return Predicates.exists();
    } else {
      return null;
    }
  }

  @Override
  public int compare(String o1, String o2) {
    return o1.compareTo(o2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    DateFormatPartitioner that = (DateFormatPartitioner) o;
    return Objects.equal(this.getSourceName(), that.getSourceName()) &&
        Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.format, that.format) &&
        Objects.equal(this.getCardinality(), that.getCardinality());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getSourceName(), getName(), format, getCardinality());
  }

}
