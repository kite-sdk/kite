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

import java.util.List;
import org.kitesdk.data.FieldPartitioner;
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

  @Beta
  public static String toExpression(FieldPartitioner fieldPartitioner) {
    // TODO: add other strategies
    if (fieldPartitioner instanceof HashFieldPartitioner) {
      return String.format("hash(\"%s\", \"%s\", %s)", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName(), fieldPartitioner.getCardinality());
    } else if (fieldPartitioner instanceof IdentityFieldPartitioner) {
      return String.format("identity(\"%s\", %s)", fieldPartitioner.getName(),
          fieldPartitioner.getCardinality());
    } else if (fieldPartitioner instanceof RangeFieldPartitioner) {
      List<String> upperBounds = ((RangeFieldPartitioner) fieldPartitioner)
          .getUpperBounds();

      StringBuilder builder = new StringBuilder();

      for (String bound : upperBounds) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append("\"").append(bound).append("\"");
      }

      return String.format("range(\"%s\", %s", fieldPartitioner.getName(),
          builder.toString());
    } else if (fieldPartitioner instanceof DateFormatPartitioner) {
      return String.format("dateFormat(\"%s\", \"%s\", \"%s\")",
          fieldPartitioner.getSourceName(),
          fieldPartitioner.getName(),
          ((DateFormatPartitioner) fieldPartitioner).getPattern());
    } else if (fieldPartitioner instanceof YearFieldPartitioner) {
      return String.format("year(\"%s\", \"%s\")", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName());
    } else if (fieldPartitioner instanceof MonthFieldPartitioner) {
      return String.format("month(\"%s\", \"%s\")", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName());
    } else if (fieldPartitioner instanceof DayOfMonthFieldPartitioner) {
      return String.format("day(\"%s\", \"%s\")", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName());
    } else if (fieldPartitioner instanceof HourFieldPartitioner) {
      return String.format("hour(\"%s\", \"%s\")", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName());
    } else if (fieldPartitioner instanceof MinuteFieldPartitioner) {
      return String.format("minute(\"%s\", \"%s\")", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName());
    }

    throw new IllegalArgumentException("Unrecognized PartitionStrategy: "
        + fieldPartitioner);
  }
}
