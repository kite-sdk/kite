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

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.Range;
import java.util.Calendar;
import org.kitesdk.data.spi.Predicates;

@Beta
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
public class YearFieldPartitioner extends CalendarFieldPartitioner {
  public YearFieldPartitioner(String sourceName, String name) {
    super(sourceName, name, Calendar.YEAR, 5); // arbitrary number of partitions
  }

  @Override
  public Predicate<Integer> project(Predicate<Long> predicate) {
    // year is the only time field that can be projected
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.In) {
      return ((Predicates.In<Long>) predicate).transform(this);
    } else if (predicate instanceof Range) {
      return Predicates.transformClosed((Range<Long>) predicate, this);
    } else {
      return null;
    }
  }
}
