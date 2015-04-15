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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
import org.codehaus.jackson.node.NullNode;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;
import org.kitesdk.data.spi.partition.ProvidedFieldPartitioner;

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
    return (Class<S>) getClassForType(fieldSchema(schema, fp.getSourceName()).getType());
  }

  @SuppressWarnings("unchecked")
  public static <S, T> Class<? extends T> getPartitionType(FieldPartitioner<S, T> fp, Schema schema) {
    if (fp instanceof ProvidedFieldPartitioner) {
      // provided partitioners have no source field schema
      return fp.getType();
    }
    Class<? extends S> inputType = (Class<S>) getClassForType(
        fieldSchema(schema, fp.getSourceName()).getType());
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
      return fieldSchema(schema, fp.getSourceName());
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
        (strategy != null && Accessor.getDefault().hasPartitioner(strategy, name)));
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
    if (strategy != null && Accessor.getDefault().hasPartitioner(strategy, name)) {
      return partitionFieldSchema(Accessor.getDefault().getPartitioner(strategy, name),
          schema);
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
    Schema nested = unwrapNullable(schema);
    List<String> levels = Lists.newArrayList();
    for (String level : NAME_SPLITTER.split(name)) {
      levels.add(level);
      ValidationException.check(Schema.Type.RECORD == schema.getType(),
          "Cannot get schema for %s: %s is not a record schema: %s",
          name, NAME_JOINER.join(levels), nested.toString(true));
      Schema.Field field = nested.getField(level);
      ValidationException.check(field != null,
          "Cannot get schema for %s: %s is not a field",
          name, NAME_JOINER.join(levels));
      nested = unwrapNullable(field.schema());
    }
    return nested;
  }

  private static Schema unwrapNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2) {
      List<Schema> types = schema.getTypes();
      if (types.get(0).getType() == Schema.Type.NULL) {
        return types.get(1);
      } else if (types.get(1).getType() == Schema.Type.NULL) {
        return types.get(0);
      }
    }
    return schema;
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
    for (FieldPartitioner<?, ?> fp : Accessor.getDefault().getFieldPartitioners(strategy)) {
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
        Accessor.getDefault().hasPartitioner(descriptor.getPartitionStrategy(), fieldName),
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

  /**
   * Merges {@link Schema} instances if they are compatible.
   * <p>
   * Schemas are incompatible if:
   * <ul>
   * <li>The {@link Schema.Type} does not match.</li>
   * <li>For record schemas, the record name does not match</li>
   * <li>For enum schemas, the enum name does not match</li>
   * </ul>
   * <p>
   * Map value, array element, and record field types types will use unions if
   * necessary, and union schemas are merged recursively.
   *
   * @param schemas a set of {@code Schema} instances to merge
   * @return a merged {@code Schema}
   * @throws IncompatibleSchemaException if the schemas are not compatible
   */
  public static Schema merge(Iterable<Schema> schemas) {
    Iterator<Schema> iter = schemas.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    Schema result = iter.next();
    while (iter.hasNext()) {
      result = merge(result, iter.next());
    }
    return result;
  }

  /**
   * Merges {@link Schema} instances and creates a union of schemas if any are
   * incompatible.
   * <p>
   * Schemas are incompatible if:
   * <ul>
   * <li>The {@link Schema.Type} does not match.</li>
   * <li>For record schemas, the record name does not match</li>
   * <li>For enum schemas, the enum name does not match</li>
   * </ul>
   * <p>
   * Map value, array element, and record field types types will use unions if
   * necessary, and union schemas are merged recursively.
   *
   * @param schemas a set of {@code Schema} instances to merge
   * @return a combined {@code Schema}
   */
  public static Schema mergeOrUnion(Iterable<Schema> schemas) {
    Iterator<Schema> iter = schemas.iterator();
    if (!iter.hasNext()) {
      return null;
    }
    Schema result = iter.next();
    while (iter.hasNext()) {
      result = mergeOrUnion(result, iter.next());
    }
    return result;
  }

  /**
   * Merges two {@link Schema} instances if they are compatible.
   * <p>
   * Two schemas are incompatible if:
   * <ul>
   * <li>The {@link Schema.Type} does not match.</li>
   * <li>For record schemas, the record name does not match</li>
   * <li>For enum schemas, the enum name does not match</li>
   * </ul>
   * <p>
   * Map value and array element types will use unions if necessary, and union
   * schemas are merged recursively.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a merged {@code Schema}
   * @throws IncompatibleSchemaException if the schemas are not compatible
   */
  public static Schema merge(Schema left, Schema right) {
    Schema merged = mergeOnly(left, right);
    IncompatibleSchemaException.check(merged != null,
        "Cannot merge %s and %s", left, right);
    return merged;
  }

  /**
   * Merges two {@link Schema} instances or returns {@code null}.
   * <p>
   * The two schemas are merged if they are the same type. Records are merged
   * if the two records have the same name or have no names but have a
   * significant number of shared fields.
   * <p>
   * @see {@link #mergeOrUnion} to return a union when a merge is not possible.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a {@code Schema} for both types
   */
  private static Schema mergeOrUnion(Schema left, Schema right) {
    Schema merged = mergeOnly(left, right);
    if (merged != null) {
      return merged;
    }
    return union(left, right);
  }

  /**
   * Creates a union of two {@link Schema} instances.
   * <p>
   * If either {@code Schema} is a union, this will attempt to merge the other
   * schema with the types contained in that union before adding more types to
   * the union that is produced.
   * <p>
   * If both schemas are not unions, no merge is attempted.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a UNION schema of the to {@code Schema} instances
   */
  private static Schema union(Schema left, Schema right) {
    if (left.getType() == Schema.Type.UNION) {
      if (right.getType() == Schema.Type.UNION) {
        // combine the unions by adding each type in right individually
        Schema combined = left;
        for (Schema type : right.getTypes()) {
          combined = union(combined, type);
        }
        return combined;

      } else {
        boolean notMerged = true;
        // combine a union with a non-union by checking if each type will merge
        List<Schema> types = Lists.newArrayList();
        Iterator<Schema> schemas = left.getTypes().iterator();
        // try to merge each type and stop when one succeeds
        while (schemas.hasNext()) {
          Schema next = schemas.next();
          Schema merged = mergeOnly(next, right);
          if (merged != null) {
            types.add(merged);
            notMerged = false;
            break;
          } else {
            // merge didn't work, add the type
            types.add(next);
          }
        }
        // add the remaining types from the left union
        while (schemas.hasNext()) {
          types.add(schemas.next());
        }

        if (notMerged) {
          types.add(right);
        }

        return Schema.createUnion(types);
      }
    } else if (right.getType() == Schema.Type.UNION) {
      return union(right, left);
    }

    return Schema.createUnion(ImmutableList.of(left, right));
  }

  /**
   * Merges two {@link Schema} instances or returns {@code null}.
   * <p>
   * The two schemas are merged if they are the same type. Records are merged
   * if the two records have the same name or have no names but have a
   * significant number of shared fields.
   * <p>
   * @see {@link #mergeOrUnion} to return a union when a merge is not possible.
   *
   * @param left a {@code Schema}
   * @param right a {@code Schema}
   * @return a merged {@code Schema} or {@code null} if merging is not possible
   */
  private static Schema mergeOnly(Schema left, Schema right) {
    if (Objects.equal(left, right)) {
      return left;
    }

    // handle primitive type promotion; doesn't promote integers to floats
    switch (left.getType()) {
      case INT:
        if (right.getType() == Schema.Type.LONG) {
          return right;
        }
        break;
      case LONG:
        if (right.getType() == Schema.Type.INT) {
          return left;
        }
        break;
      case FLOAT:
        if (right.getType() == Schema.Type.DOUBLE) {
          return right;
        }
        break;
      case DOUBLE:
        if (right.getType() == Schema.Type.FLOAT) {
          return left;
        }
    }

    // any other cases where the types don't match must be combined by a union
    if (left.getType() != right.getType()) {
      return null;
    }

    switch (left.getType()) {
      case UNION:
        return union(left, right);
      case RECORD:
        if (left.getName() == null && right.getName() == null &&
            fieldSimilarity(left, right) < SIMILARITY_THRESH) {
          return null;
        } else if (!Objects.equal(left.getName(), right.getName())) {
          return null;
        }

        Schema combinedRecord = Schema.createRecord(
            coalesce(left.getName(), right.getName()),
            coalesce(left.getDoc(), right.getDoc()),
            coalesce(left.getNamespace(), right.getNamespace()),
            false
        );
        combinedRecord.setFields(mergeFields(left, right));

        return combinedRecord;

      case MAP:
        return Schema.createMap(
            mergeOrUnion(left.getValueType(), right.getValueType()));

      case ARRAY:
        return Schema.createArray(
            mergeOrUnion(left.getElementType(), right.getElementType()));

      case ENUM:
        if (!Objects.equal(left.getName(), right.getName())) {
          return null;
        }
        Set<String> symbols = Sets.newLinkedHashSet();
        symbols.addAll(left.getEnumSymbols());
        symbols.addAll(right.getEnumSymbols());
        return Schema.createEnum(
            left.getName(),
            coalesce(left.getDoc(), right.getDoc()),
            coalesce(left.getNamespace(), right.getNamespace()),
            ImmutableList.copyOf(symbols)
        );

      default:
        // all primitives are handled before the switch by the equality check.
        // schemas that reach this point are not primitives and also not any of
        // the above known types.
        throw new UnsupportedOperationException(
            "Unknown schema type: " + left.getType());
    }
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private static final NullNode NULL_DEFAULT = NullNode.getInstance();

  /**
   * Returns a union {@link Schema} of NULL and the given {@code schema}.
   * <p>
   * A NULL schema is always the first type in the union so that a null default
   * value can be set.
   *
   * @param schema a {@code Schema}
   * @return a union of null and the given schema
   */
  private static Schema nullableForDefault(Schema schema) {
    if (schema.getType() == Schema.Type.NULL) {
      return schema;
    }

    if (schema.getType() != Schema.Type.UNION) {
      return Schema.createUnion(ImmutableList.of(NULL, schema));
    }

    if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      return schema;
    }

    List<Schema> types = Lists.newArrayList();
    types.add(NULL);
    for (Schema type : schema.getTypes()) {
      if (type.getType() != Schema.Type.NULL) {
        types.add(type);
      }
    }

    return Schema.createUnion(types);
  }

  private static List<Schema.Field> mergeFields(Schema left, Schema right) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field leftField : left.getFields()) {
      Schema.Field rightField = right.getField(leftField.name());
      if (rightField != null) {
        fields.add(new Schema.Field(
            leftField.name(),
            mergeOrUnion(leftField.schema(), rightField.schema()),
            coalesce(leftField.doc(), rightField.doc()),
            coalesce(leftField.defaultValue(), rightField.defaultValue())
        ));
      } else {
        if (leftField.defaultValue() != null) {
          fields.add(copy(leftField));
        } else {
          fields.add(new Schema.Field(
              leftField.name(), nullableForDefault(leftField.schema()),
              leftField.doc(), NULL_DEFAULT
          ));
        }
      }
    }

    for (Schema.Field rightField : right.getFields()) {
      if (left.getField(rightField.name()) == null) {
        if (rightField.defaultValue() != null) {
          fields.add(copy(rightField));
        } else {
          fields.add(new Schema.Field(
              rightField.name(), nullableForDefault(rightField.schema()),
              rightField.doc(), NULL_DEFAULT
          ));
        }
      }
    }

    return fields;
  }

  /**
   * Creates a new field with the same name, schema, doc, and default value as
   * the incoming schema.
   * <p>
   * Fields cannot be used in more than one record (not Immutable?).
   */
  public static Schema.Field copy(Schema.Field field) {
    return new Schema.Field(
        field.name(), field.schema(), field.doc(), field.defaultValue());
  }

  private static float fieldSimilarity(Schema left, Schema right) {
    // check whether the unnamed records appear to be the same record
    Set<String> leftNames = names(left.getFields());
    Set<String> rightNames = names(right.getFields());
    int common = Sets.intersection(leftNames, rightNames).size();
    float leftRatio = ((float) common) / ((float) leftNames.size());
    float rightRatio = ((float) common) / ((float) rightNames.size());
    return hmean(leftRatio, rightRatio);
  }

  private static Set<String> names(Collection<Schema.Field> fields) {
    Set<String> names = Sets.newHashSet();
    for (Schema.Field field : fields) {
      names.add(field.name());
    }
    return names;
  }
  private static float SIMILARITY_THRESH = 0.3f;
  private static float hmean(float left, float right) {
    return (2.0f * left * right) / (left + right);
  }

  /**
   * Returns the first non-null object that is passed in.
   */
  private static <E> E coalesce(E... objects) {
    for (E object : objects) {
      if (object != null) {
        return object;
      }
    }
    return null;
  }

  /**
   * Returns whether null is allowed by the schema.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  public static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }
}
