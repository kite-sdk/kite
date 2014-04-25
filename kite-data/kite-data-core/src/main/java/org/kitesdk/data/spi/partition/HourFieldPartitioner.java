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

import java.text.NumberFormat;
import java.util.Calendar;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", "EQ_DOESNT_OVERRIDE_EQUALS"},
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization."
        + "EQ: parent equals implementation checks child type")
@Immutable
public class HourFieldPartitioner extends CalendarFieldPartitioner {
  private final NumberFormat format;

  public HourFieldPartitioner(String sourceName) {
    this(sourceName, null);
  }

  public HourFieldPartitioner(String sourceName, @Nullable String name) {
    super(sourceName, (name == null ? "hour" : name),
        Calendar.HOUR_OF_DAY, 24);
    format = NumberFormat.getIntegerInstance();
    format.setMinimumIntegerDigits(2);
    format.setMaximumIntegerDigits(2);
  }

  @Override
  @Deprecated
  public String valueToString(Integer value) {
    return format.format(value);
  }
}
