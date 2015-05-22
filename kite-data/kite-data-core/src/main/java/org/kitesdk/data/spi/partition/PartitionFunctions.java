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

import java.util.List;
import javax.annotation.Nullable;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.FieldPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class so you can say, for example, <code>hash("username", 2)</code> in
 * JEXL.
 */
@SuppressWarnings("unchecked")
public class PartitionFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionFunctions.class);

  public static FieldPartitioner<Object, Integer> hash(String name, int buckets) {
    return new HashFieldPartitioner(name, buckets);
  }

  public static FieldPartitioner<Object, Integer> hash(String sourceName, @Nullable String name, int buckets) {
    return new HashFieldPartitioner(sourceName, name, buckets);
  }

  public static FieldPartitioner identity(String sourceName, int buckets) {
    return new IdentityFieldPartitioner(sourceName, Object.class, buckets);
  }

  public static FieldPartitioner identity(String sourceName, String name, String className, int buckets) {
    Class<?> typeClass;
    try {
      typeClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new DatasetException("Cannot find class: " + className, e);
    }
    return new IdentityFieldPartitioner(sourceName, name, typeClass, buckets);
  }

  public static FieldPartitioner<Integer, Integer> range(String sourceName, @Nullable String name, int... upperBounds) {
    return new IntRangeFieldPartitioner(sourceName, name, upperBounds);
  }

  public static FieldPartitioner<String, String> range(String sourceName, @Nullable String name, String... upperBounds) {
    return new RangeFieldPartitioner(sourceName, name, upperBounds);
  }

  public static FieldPartitioner<Long, Long> fixedSizeRange(String sourceName, @Nullable String name, long range) {
    return new LongFixedSizeRangeFieldPartitioner(sourceName, name, range);
  }

  public static FieldPartitioner<Long, Integer> year(String sourceName, @Nullable String name) {
    return new YearFieldPartitioner(sourceName, name);
  }

  public static FieldPartitioner<Long, Integer> month(String sourceName, @Nullable String name) {
    return new MonthFieldPartitioner(sourceName, name);
  }

  public static FieldPartitioner<Long, Integer> day(String sourceName, @Nullable String name) {
    return new DayOfMonthFieldPartitioner(sourceName, name);
  }

  public static FieldPartitioner<Long, Integer> hour(String sourceName, @Nullable String name) {
    return new HourFieldPartitioner(sourceName, name);
  }

  public static FieldPartitioner<Long, Integer> minute(String sourceName, @Nullable String name) {
    return new MinuteFieldPartitioner(sourceName, name);
  }

  public static FieldPartitioner<Long, String> dateFormat(String sourceName, String name, String format) {
    return new DateFormatPartitioner(sourceName, name, format);
  }

  public static FieldPartitioner provided(String name, @Nullable String valuesType) {
    return new ProvidedFieldPartitioner(name,
        ProvidedFieldPartitioner.valuesType(valuesType));
  }

  public static String toExpression(FieldPartitioner fieldPartitioner) {
    // TODO: add other strategies
    if (fieldPartitioner instanceof HashFieldPartitioner) {
      return String.format("hash(\"%s\", \"%s\", %s)", fieldPartitioner.getSourceName(),
          fieldPartitioner.getName(), fieldPartitioner.getCardinality());
    } else if (fieldPartitioner instanceof IdentityFieldPartitioner) {
      return String.format("identity(\"%s\", \"%s\", \"%s\", %s)",
          fieldPartitioner.getSourceName(), fieldPartitioner.getName(),
          fieldPartitioner.getType().getName(),
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

      return String.format("range(\"%s\", \"%s\", %s)",
          fieldPartitioner.getSourceName(), fieldPartitioner.getName(),
          builder.toString());
    } else if (fieldPartitioner instanceof IntRangeFieldPartitioner) {
      StringBuilder builder = new StringBuilder();

      for (int bound : ((IntRangeFieldPartitioner) fieldPartitioner).getUpperBounds()) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(bound);
      }

      return String.format("range(\"%s\", \"%s\", %s)",
          fieldPartitioner.getSourceName(), fieldPartitioner.getName(),
          builder.toString());
    } else if (fieldPartitioner instanceof LongFixedSizeRangeFieldPartitioner) {
      return String.format("fixedSizeRange(\"%s\", \"%s\", %s)",
          fieldPartitioner.getSourceName(), fieldPartitioner.getName(),
          ((LongFixedSizeRangeFieldPartitioner) fieldPartitioner).getSize());
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
    } else if (fieldPartitioner instanceof ProvidedFieldPartitioner) {
      return String.format("provided(\"%s\", \"%s\")",
          fieldPartitioner.getName(),
          ((ProvidedFieldPartitioner) fieldPartitioner).getTypeAsString());
    }

    throw new IllegalArgumentException("Unrecognized partition function: "
        + fieldPartitioner);
  }
}
