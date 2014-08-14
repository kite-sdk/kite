/*
 * Copyright 2013 Cloudera.
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

package org.kitesdk.data.spi;

import com.google.common.base.Preconditions;
import java.text.NumberFormat;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * Static helper methods for converting between types.
 *
 * @since 0.9.0
 */
public class Conversions {

  public static <T> T convert(Object obj, Class<T> returnType) {
    if (obj == null) {
      return null;
    }

    if (returnType == Object.class) {
      return returnType.cast(obj);
    } else if (returnType.isAssignableFrom(Long.class)) {
      return returnType.cast(makeLong(obj));
    } else if (returnType.isAssignableFrom(Integer.class)) {
      return returnType.cast(makeInteger(obj));
    } else if (returnType.isAssignableFrom(String.class)) {
      return returnType.cast(makeString(obj));
    } else if (returnType.isAssignableFrom(Double.class)) {
      return returnType.cast(makeDouble(obj));
    } else if (returnType.isAssignableFrom(Float.class)) {
      return returnType.cast(makeFloat(obj));
    } else {
      throw new ClassCastException(
          "Cannot convert to unknown return type:" + returnType.getName());
    }
  }

  public static Object convert(Object obj, Schema schema) {
    Class<?> returnType = SchemaUtil.getClassForType(schema.getType());
    if (returnType != null) {
      return convert(obj, returnType);
    }

    switch (schema.getType()) {
      case UNION:
        for (Schema possibleSchema : schema.getTypes()) {
          try {
            return convert(obj,
                SchemaUtil.getClassForType(possibleSchema.getType()));
          } catch (ClassCastException e) {
            // didn't work, try the next one
          }
        }
        break;
    }
    throw new ClassCastException(
        "Cannot convert using schema: " + schema.toString(true));
  }

  public static Long makeLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Long");
    }
  }

  public static Integer makeInteger(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Integer");
    }
  }

  public static Double makeDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Double");
    }
  }

  public static Float makeFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.valueOf((String) value);
    } else {
      throw new RuntimeException(
          "Cannot coerce \"" + value + "\" to Double");
    }
  }

  public static String makeString(Object value) {
    // start simple, but we may want more complicated conversion here if, for
    // example, we needed to support Writables or other serializations
    return value.toString();
  }

  public static String makeString(Object value, Integer width) {
    if (width != null && value instanceof Number) {
      NumberFormat format = NumberFormat.getInstance();
      format.setMinimumIntegerDigits(width);
      format.setGroupingUsed(false);
      return format.format(value);
    } else {
      return makeString(value);
    }
  }

}
