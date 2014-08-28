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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.codec.binary.Base64;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;

public class SchemaUtil {

  static final Splitter NAME_SPLITTER = Splitter.on('.');
  static final Joiner NAME_JOINER = Joiner.on('.');

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
    checkTypeConsistency(schema, null, fieldName, values);
  }

  /**
   * Checks that the type of each of {@code values} is consistent with the type of
   * field {@code fieldName} declared in the Avro schema (from {@code descriptor}).
   */
  public static void checkTypeConsistency(Schema schema,
                                          PartitionStrategy strategy,
                                          String fieldName, Object... values) {
    Schema fieldSchema = fieldSchema(schema, strategy, fieldName);

    for (Object value : values) {
      // SpecificData#validate checks consistency for generic, reflect,
      // and specific models.
      Preconditions.checkArgument(
          SpecificData.get().validate(fieldSchema, value),
          "Value '%s' of type '%s' inconsistent with field schema %s.",
          value, value.getClass(), fieldSchema);
    }
  }

  /**
   * Builds a Schema for the FieldPartitioner using the given Schema to
   * determine types not fixed by the FieldPartitioner.
   *
   * @param fp a FieldPartitioner
   * @param schema an entity Schema that will be partitioned
   * @return a Schema for the field partitioner
   */
  public static Schema partitionFieldSchema(FieldPartitioner<?, ?> fp, Schema schema) {
    if (fp instanceof IdentityFieldPartitioner) {
      // copy the schema directly from the entity to preserve annotations
      return schema.getField(fp.getSourceName()).schema();
    } else {
      Class<?> fieldType = getPartitionType(fp, schema);
      if (fieldType == Integer.class) {
        return Schema.create(Schema.Type.INT);
      } else if (fieldType == Long.class) {
        return Schema.create(Schema.Type.LONG);
      } else if (fieldType == String.class) {
        return Schema.create(Schema.Type.STRING);
      } else {
        throw new ValidationException(
            "Cannot encode partition " + fp.getName() +
                " with type " + fp.getSourceType()
        );
      }
    }
  }

  /**
   * Return whether there is a field named {@code name} in the schema or
   * partition strategy.
   *
   * @param schema a {@code Schema}
   * @param strategy a {@code PartitionStrategy}
   * @param name a String field name
   * @return {@code true} if {@code name} identifies a field
   */
  public static boolean isField(Schema schema, PartitionStrategy strategy,
                                String name) {
    Schema.Field field = schema.getField(name);
    return ((field != null) ||
        (strategy != null && strategy.hasPartitioner(name)));
  }

  /**
   * Returns a {@link Schema} for the given field name, which could be either a
   * schema field or a partition field.
   *
   * @param schema an entity Schema that will be partitioned
   * @param strategy a {@code PartitionStrategy} used to partition entities
   * @param name a schema or partition field name
   * @return a Schema for the partition or schema field
   */
  public static Schema fieldSchema(Schema schema, PartitionStrategy strategy,
                                   String name) {
    if (strategy != null && strategy.hasPartitioner(name)) {
      return partitionFieldSchema(strategy.getPartitioner(name), schema);
    }
    Schema nested = fieldSchema(schema, name);
    if (nested != null) {
      return nested;
    }
    throw new IllegalArgumentException(
        "Not a schema or partition field: " + name);
  }

  /**
   * Returns the nested {@link Schema} for the given field name.
   *
   * @param schema a record Schema
   * @param name a String field name
   * @return the nested Schema for the field
   */
  public static Schema fieldSchema(Schema schema, String name) {
    Schema nested = schema;
    List<String> levels = Lists.newArrayList();
    for (String level : NAME_SPLITTER.split(name)) {
      levels.add(level);
      Preconditions.checkArgument(Schema.Type.RECORD == schema.getType(),
          "Cannot get schema for %s: %s is not a record schema: %s",
          name, NAME_JOINER.join(levels), nested.toString(true));
      Schema.Field field = nested.getField(level);
      Preconditions.checkArgument(field != null,
          "Cannot get schema for %s: %s is not a field",
          name, NAME_JOINER.join(levels));
      nested = field.schema();
    }
    return nested;
  }

  /**
   * Creates a {@link Schema} for the keys of a {@link PartitionStrategy} based
   * on an entity {@code Schema}.
   * <p>
   * The partition strategy and schema are assumed to be compatible and this
   * will result in NullPointerExceptions if they are not.
   *
   * @param schema an entity schema {@code Schema}
   * @param strategy a {@code PartitionStrategy}
   * @return a {@code Schema} for the storage keys of the partition strategy
   */
  public static Schema keySchema(Schema schema, PartitionStrategy strategy) {
    List<Schema.Field> partitionFields = new ArrayList<Schema.Field>();
    for (FieldPartitioner<?, ?> fp : strategy.getFieldPartitioners()) {
      partitionFields.add(partitionField(fp, schema));
    }
    Schema keySchema = Schema.createRecord(
        schema.getName() + "KeySchema", null, null, false);
    keySchema.setFields(partitionFields);
    return keySchema;
  }

  /**
   * Builds a Schema.Field for the FieldPartitioner using the Schema to
   * determine types not fixed by the FieldPartitioner.
   *
   * @param fp a FieldPartitioner
   * @param schema an entity Schema that will be partitioned
   * @return a Schema.Field for the field partitioner, with the same name
   */
  private static Schema.Field partitionField(FieldPartitioner<?, ?> fp,
                                             Schema schema) {
    return new Schema.Field(
        fp.getName(), partitionFieldSchema(fp, schema), null, null);
  }

  public static void checkPartitionedBy(DatasetDescriptor descriptor,
                                        String fieldName) {
    Preconditions.checkArgument(descriptor.isPartitioned(),
        "Descriptor %s is not partitioned", descriptor);
    Preconditions.checkArgument(
        descriptor.getPartitionStrategy().hasPartitioner(fieldName),
        "Descriptor %s is not partitioned by '%s'", descriptor, fieldName);
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

  @SuppressWarnings("unchecked")
  public static String toString(Object value, Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value.toString();
      case STRING:
        // TODO: could be null
        try {
          return URLEncoder.encode(value.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new DatasetIOException("Failed to encode value: " + value, e);
        }
      default:
        // otherwise, encode as Avro binary and then base64
        DatumWriter writer = ReflectData.get().createDatumWriter(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
          writer.write(value, encoder);
          encoder.flush();
        } catch (IOException e) {
          throw new DatasetIOException("Cannot encode Avro value", e);
        }
        return Base64.encodeBase64URLSafeString(out.toByteArray());
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T fromString(String value, Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return (T) Boolean.valueOf(value);
      case INT:
        return (T) Integer.valueOf(value);
      case LONG:
        return (T) Long.valueOf(value);
      case FLOAT:
        return (T) Float.valueOf(value);
      case DOUBLE:
        return (T) Double.valueOf(value);
      case STRING:
        try {
          return (T) URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new DatasetIOException("Failed to decode value: " + value, e);
        }
      default:
        //  otherwise, the value is Avro binary encoded with base64
        byte[] binary = Base64.decodeBase64(value);
        Decoder decoder = DecoderFactory.get()
            .binaryDecoder(new ByteArrayInputStream(binary), null);
        DatumReader<T> reader = ReflectData.get().createDatumReader(schema);
        try {
          return reader.read(null, decoder);
        } catch (IOException e) {
          throw new DatasetIOException("Cannot decode Avro value", e);
        }
    }
  }
}
