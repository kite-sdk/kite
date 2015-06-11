/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/**
 * Utilities for determining the appropriate data model at runtime.
 *
 * @since 0.15.0
 */
public class DataModelUtil {

  // Replace this with ReflectData.AllowNull once AVRO-1589 is available
  @VisibleForTesting
  static class AllowNulls extends ReflectData {
    private static final AllowNulls INSTANCE = new AllowNulls();

    /** Return the singleton instance. */
    public static AllowNulls get() { return INSTANCE; }

    @Override
    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
      Schema schema = super.createFieldSchema(field, names);
      if (field.getType().isPrimitive()) {
        return schema;
      }
      return makeNullableSchema(schema);
    }

    /** Create and return a union of the null schema and the provided schema. */
    public static Schema makeNullableSchema(Schema schema) {
      if (schema.getType() == Schema.Type.UNION) {
        // check to see if the union already contains NULL
        for (Schema subType : schema.getTypes()) {
          if (subType.getType() == Schema.Type.NULL) {
            return schema;
          }
        }
        // add null as the first type in a new union
        List<Schema> withNull = new ArrayList<Schema>();
        withNull.add(Schema.create(Schema.Type.NULL));
        withNull.addAll(schema.getTypes());
        return Schema.createUnion(withNull);
      } else {
        // create a union with null
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
            schema));
      }
    }
  }

  /**
   * Get the data model for the given type.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @return The appropriate data model for the given type
   */
  public static <E> GenericData getDataModelForType(Class<E> type) {
    // Need to check if SpecificRecord first because specific records also
    // implement GenericRecord
    if (SpecificRecord.class.isAssignableFrom(type)) {
      return new SpecificData(type.getClassLoader());
    } else if (IndexedRecord.class.isAssignableFrom(type)) {
      return GenericData.get();
    } else {
      return AllowNulls.get();
    }
  }

  /**
   * Test whether the class is a generic {@link GenericRecord} and not a
   * {@link SpecificRecord}. This is necessary instead of testing if
   * {@code GenericRecord} is assignable from the class because
   * {@link org.apache.avro.specific.SpecificRecordBase} implements
   * {@code GenericRecord}.
   *
   * @param type a Java class
   * @return true if the class is an Avro generic and not specific
   */
  public static boolean isGeneric(Class<?> type) {
    return (
        !SpecificRecord.class.isAssignableFrom(type) &&
            GenericRecord.class.isAssignableFrom(type)
    );
  }

  /**
   * Get the DatumReader for the given type.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @param writerSchema The {@link Schema} for entities
   * @return The DatumReader for the given type
   */
  @SuppressWarnings("unchecked")
  public static <E> DatumReader<E> getDatumReaderForType(Class<E> type, Schema writerSchema) {
    Schema readerSchema = getReaderSchema(type, writerSchema);
    GenericData dataModel = getDataModelForType(type);
    if (dataModel instanceof ReflectData) {
      return new ReflectDatumReader<E>(writerSchema, readerSchema, (ReflectData)dataModel);
    } else if (dataModel instanceof SpecificData) {
      return new SpecificDatumReader<E>(writerSchema, readerSchema, (SpecificData)dataModel);
    } else {
      return new GenericDatumReader<E>(writerSchema, readerSchema, dataModel);
    }
  }

  /**
   * Resolves the type based on the given schema. In most cases, the type should
   * stay as is. However, if the type is Object, then that means that the old
   * default behavior of determining the class from ReflectData#getClass(Schema)
   * should be used. If a class can't be found, it will default to
   * GenericData.Record.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @param schema The {@link Schema} for the entity
   * @return The resolved Java class object
   */
  @SuppressWarnings("unchecked")
  public static <E> Class<E> resolveType(Class<E> type, Schema schema) {
    if (type == Object.class) {
      type = ReflectData.get().getClass(schema);
    }

    if (type == null) {
      type = (Class<E>) GenericData.Record.class;
    }

    return type;
  }

  /**
   * Get the reader schema based on the given type and writer schema.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @param schema The {@link Schema} for the entity
   * @return The reader schema based on the given type and writer schema
   */
  public static <E> Schema getReaderSchema(Class<E> type, Schema schema) {
    Schema readerSchema = schema;
    GenericData dataModel = getDataModelForType(type);

    if (dataModel instanceof SpecificData) {
      readerSchema = ((SpecificData)dataModel).getSchema(type);
    }

    return readerSchema;
  }

  /**
   * Get the writer schema based on the given type and dataset schema.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @param schema The {@link Schema} for the entity
   * @return The reader schema based on the given type and writer schema
   */
  public static <E> Schema getWriterSchema(Class<E> type, Schema schema) {
    Schema writerSchema = schema;
    GenericData dataModel = getDataModelForType(type);
    if (dataModel instanceof AllowNulls) {
      // assume fields are non-null by default to avoid schema conflicts
      dataModel = ReflectData.get();
    }

    if (dataModel instanceof SpecificData) {
      writerSchema = ((SpecificData)dataModel).getSchema(type);
    }

    return writerSchema;
  }

  /**
   * If E implements GenericRecord, but does not implement SpecificRecord, then
   * create a new instance of E using reflection so that GenericDataumReader
   * will use the expected type.
   *
   * Implementations of GenericRecord that require a {@link Schema} parameter
   * in the constructor should implement SpecificData.SchemaConstructable.
   * Otherwise, your implementation must have a no-args constructor.
   *
   * @param <E> The entity type
   * @param type The Java class of the entity type
   * @param schema The reader schema
   * @return An instance of E, or null if the data model is specific or reflect
   */
  @SuppressWarnings("unchecked")
  public static <E> E createRecord(Class<E> type, Schema schema) {
    // Don't instantiate SpecificRecords or interfaces.
    if (isGeneric(type) && !type.isInterface()) {
      if (GenericData.Record.class.equals(type)) {
        return (E) GenericData.get().newRecord(null, schema);
      }
      return (E) ReflectData.newInstance(type, schema);
    }

    return null;
  }

  public static <E> EntityAccessor<E> accessor(Class<E> type, Schema schema) {
    return new EntityAccessor<E>(type, schema);
  }
}
