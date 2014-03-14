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

package org.kitesdk.data.spi;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;

public class SchemaUtil {

  public static final ImmutableMap<Schema.Type, Class<?>> TYPE_TO_CLASS =
      ImmutableMap.<Schema.Type, Class<?>>builder()
          .put(Schema.Type.BOOLEAN, Boolean.class)
          .put(Schema.Type.INT, Integer.class)
          .put(Schema.Type.LONG, Long.class)
          .put(Schema.Type.FLOAT, Float.class)
          .put(Schema.Type.DOUBLE, Double.class)
          .put(Schema.Type.STRING, String.class)
          .put(Schema.Type.BYTES, ByteBuffer.class)
          .build();

  public static boolean checkType(Schema.Type type, Class<?> expectedClass) {
    Class<?> typeClass = TYPE_TO_CLASS.get(type);
    return typeClass != null && expectedClass.isAssignableFrom(typeClass);
  }

  public abstract static class SchemaVisitor<T> {
    protected LinkedList<String> recordLevels = Lists.newLinkedList();

    public T record(Schema record, List<String> names, List<T> fields) {
      return null;
    }

    public T union(Schema union, List<T> options) {
      return null;
    }

    public T array(Schema array, T element) {
      return null;
    }

    public T map(Schema map, T value) {
      return null;
    }

    public T primitive(Schema primitive) {
      return null;
    }
  }

  public static <T> T visit(Schema schema, SchemaVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        // check to make sure this hasn't been visited before
        String name = schema.getFullName();
        Preconditions.checkState(!visitor.recordLevels.contains(name),
            "Cannot process recursive Avro record {}", name);

        visitor.recordLevels.push(name);

        List<Schema.Field> fields = schema.getFields();
        List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
        List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
        for (Schema.Field field : schema.getFields()) {
          names.add(field.name());
          results.add(visit(field.schema(), visitor));
        }

        visitor.recordLevels.pop();

        return visitor.record(schema, names, results);

      case UNION:
        List<Schema> types = schema.getTypes();
        List<T> options = Lists.newArrayListWithExpectedSize(types.size());
        for (Schema type : types) {
          options.add(visit(type, visitor));
        }
        return visitor.union(schema, options);

      case ARRAY:
        return visitor.array(schema, visit(schema.getElementType(), visitor));

      case MAP:
        return visitor.map(schema, visit(schema.getValueType(), visitor));

      default:
        return visitor.primitive(schema);
    }
  }

}
