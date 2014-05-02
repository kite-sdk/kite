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
import org.apache.avro.specific.SpecificData;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.PartitionStrategy;

public class SchemaUtil {

  private static final ImmutableMap<Schema.Type, Class<?>> TYPE_TO_CLASS =
      ImmutableMap.<Schema.Type, Class<?>>builder()
          .put(Schema.Type.BOOLEAN, Boolean.class)
          .put(Schema.Type.INT, Integer.class)
          .put(Schema.Type.LONG, Long.class)
          .put(Schema.Type.FLOAT, Float.class)
          .put(Schema.Type.DOUBLE, Double.class)
          .put(Schema.Type.STRING, String.class)
          .put(Schema.Type.BYTES, ByteBuffer.class)
          .build();

  public static Class<?> getClassForType(Schema.Type type) {
    return TYPE_TO_CLASS.get(type);
  }

  @SuppressWarnings("unchecked")
  public static <S> Class<? extends S> getSourceType(FieldPartitioner<S, ?> fp, Schema schema) {
    return (Class<S>) getClassForType(
        schema.getField(fp.getSourceName()).schema().getType());
  }

  @SuppressWarnings("unchecked")
  public static <S, T> Class<? extends T> getPartitionType(FieldPartitioner<S, T> fp, Schema schema) {
    Class<? extends S> inputType = (Class<S>) getClassForType(
        schema.getField(fp.getSourceName()).schema().getType());
    return fp.getType(inputType);
  }

  /**
   * Checks that a schema type should produce an object of the expected class.
   *
   * This check uses {@link Class#isAssignableFrom(Class)}.
   *
   * @param type an avro Schema.Type
   * @param expectedClass a java class
   * @return {@code true} if the type produces objects assignable to the class,
   *         {@code false} if the class is not assignable from the avro type.
   */
  public static boolean isConsistentWithExpectedType(Schema.Type type,
                                                     Class<?> expectedClass) {
    Class<?> typeClass = TYPE_TO_CLASS.get(type);
    return typeClass != null && expectedClass.isAssignableFrom(typeClass);
  }

  public static boolean isConsistentWithMappingType(
      Schema.Type type, FieldMapping.MappingType mappingType) {
    switch (mappingType) {
      case COUNTER:
      case OCC_VERSION:
        return (type == Schema.Type.INT || type == Schema.Type.LONG);
      case KEY_AS_COLUMN:
        return (type == Schema.Type.MAP || type == Schema.Type.RECORD);
      case KEY:
        // must be a primitive type
        return TYPE_TO_CLASS.containsKey(type);
      default:
        return true;
    }
  }

  /**
   * Checks that the type of each of {@code values} is consistent with the type of
   * field {@code fieldName} declared in the Avro schema (from {@code descriptor}).
   */
  public static void checkTypeConsistency(Schema schema, String fieldName,
      Object... values) {

    Preconditions.checkArgument(hasField(schema, fieldName),
        "No field '%s' in schema %s", fieldName, schema);

    Schema.Field field = schema.getField(fieldName);

    for (Object value : values) {
      // SpecificData#validate checks consistency for generic, reflect,
      // and specific models.
      Preconditions.checkArgument(SpecificData.get().validate(field.schema(), value),
          "Value '%s' of type '%s' inconsistent with field %s.", value, value.getClass(),
          field);
    }
  }

  private static boolean hasField(Schema schema, String fieldName) {
    return (schema.getField(fieldName) != null);
  }

  public static void checkPartitionedBy(DatasetDescriptor descriptor,
                                         String fieldName) {
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Descriptor %s is not partitioned", descriptor);
    Preconditions.checkArgument(isPartitionedBy(descriptor, fieldName),
        "Descriptor %s is not partitioned by '%s'", descriptor, fieldName);
  }

  private static boolean isPartitionedBy(DatasetDescriptor descriptor, String fieldName) {
    PartitionStrategy partitionStrategy = descriptor.getPartitionStrategy();
    for (FieldPartitioner<?, ?> fp : partitionStrategy.getFieldPartitioners()) {
      if (fp.getSourceName().equals(fieldName)) {
        return true;
      }
    }
    return false;
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
            "Cannot process recursive Avro record %s", name);

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
