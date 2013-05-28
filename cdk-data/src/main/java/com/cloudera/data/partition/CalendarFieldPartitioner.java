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
package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import java.util.Calendar;
import javax.annotation.Nonnull;

/**
 * A {@link FieldPartitioner} that extracts the value of a {@link Calendar} field,
 * such as {@link Calendar#YEAR}. See subclasses for convenient classes,
 * e.g. {@link YearFieldPartitioner}. Note that we don't use
 * {@link java.text.SimpleDateFormat} patterns since we want to keep the type information
 * (values are ints).
 */
@Beta
class CalendarFieldPartitioner extends FieldPartitioner {

  protected Calendar cal;
  protected int calendarField;

  public CalendarFieldPartitioner(String sourceName, String name,
      int calendarField, int cardinality) {
    super(sourceName, name, cardinality);
    this.cal = Calendar.getInstance();
    this.calendarField = calendarField;
  }

  @Override
  public Object apply(@Nonnull Object value) {
    Long timestamp = (Long) value;
    cal.setTimeInMillis(timestamp);
    return cal.get(calendarField);
  }

  @Override
  public Object valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }

  @Override
  public boolean equals(Object o) {
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
