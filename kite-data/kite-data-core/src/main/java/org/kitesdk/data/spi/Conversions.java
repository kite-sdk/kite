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

  public static Object convertField(Object obj, Schema schema, String name) {
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD,
        "Trying to convert the field of a non-Record Avro object.");
    Field field = schema.getField(name);
    Schema.Type type = field.schema().getType();
    if (type == Schema.Type.RECORD) {
      return obj;
    }

    Class<?> returnType = SchemaUtil.getClassForType(type);
    if (type == Schema.Type.UNION) {
      List<Schema> types = field.schema().getTypes();
      for(int i = 0; i < types.size(); i++) {
        type = types.get(i).getType();
        returnType = SchemaUtil.getClassForType(type);
        if (returnType != null) {
          break;
        }
      }
    }

    if (returnType == null) {
      throw new IllegalArgumentException(String.format("No valid type conversion for schema %s",
          field.schema().toString()));
    }

    return convert(obj, returnType);
  }

  public static Comparable convertField(Comparable obj, Schema schema, String name) {
    return (Comparable) convertField((Object)obj, schema, name);
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
