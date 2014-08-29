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
package org.kitesdk.data.hbase.avro;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.hbase.impl.EntityComposer;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.PartitionKey;

/**
 * An EntityComposer implementation for Avro records. It will handle both
 * SpecificRecord entities and GenericRecord entities.
 * 
 * @param <E>
 *          The type of the entity
 */
public class AvroEntityComposer<E extends IndexedRecord> implements
    EntityComposer<E> {

  /**
   * The Avro schema for the Avro records this EntityComposer will compose.
   */
  private final AvroEntitySchema avroSchema;

  /**
   * The accessor used to read fields.
   */
  private final EntityAccessor<IndexedRecord> accessor;

  /**
   * Boolean to indicate whether this is a specific record or generic record
   * composer. TODO: Eventually use an enum type when we support more than two
   * types of Avro records.
   */
  private final boolean specific;

  /**
   * An AvroRecordBuilderFactory that can produce AvroRecordBuilders for this
   * composer to compose Avro entities.
   */
  private final AvroRecordBuilderFactory<E> recordBuilderFactory;

  /**
   * A mapping of entity field names to AvroRecordBuilderFactories for any
   * keyAsColumn mapped fields that are Avro record types. These are needed to
   * get builders that can construct the keyAsColumn field values from their
   * parts.
   */
  private final Map<String, AvroRecordBuilderFactory<E>> kacRecordBuilderFactories;

  /**
   * AvroEntityComposer constructor.
   * 
   * @param avroEntitySchema
   *          The schema for the Avro entities this composer composes.
   * @param specific
   *          True if this composer composes Specific records. Otherwise, it
   *          composes Generic records.
   */
  public AvroEntityComposer(AvroEntitySchema avroEntitySchema, boolean specific) {
    this.avroSchema = avroEntitySchema;
    this.accessor = DataModelUtil.accessor(IndexedRecord.class, avroSchema.getAvroSchema());
    this.specific = specific;
    this.recordBuilderFactory = buildAvroRecordBuilderFactory(avroEntitySchema
        .getAvroSchema());
    this.kacRecordBuilderFactories = new HashMap<String, AvroRecordBuilderFactory<E>>();
    initRecordBuilderFactories();
  }

  @Override
  public Builder<E> getBuilder() {
    return new Builder<E>() {
      private final AvroRecordBuilder<E> recordBuilder = recordBuilderFactory
          .getBuilder();

      @Override
      public org.kitesdk.data.hbase.impl.EntityComposer.Builder<E> put(
          String fieldName, Object value) {
        recordBuilder.put(fieldName, value);
        return this;
      }

      @Override
      public E build() {
        return recordBuilder.build();
      }
    };
  }

  @Override
  public Object extractField(E entity, String fieldName) {
    // make sure the field is a direct child of the schema
    ValidationException.check(
        accessor.getEntitySchema().getField(fieldName) != null,
        "No field named %s in schema %s", fieldName, accessor.getEntitySchema());
    return accessor.get(entity, fieldName);
  }

  @Override
  public PartitionKey extractKey(PartitionStrategy strategy, E entity) {
    return PartitionKey.partitionKeyForEntity(strategy, entity, accessor);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<CharSequence, Object> extractKeyAsColumnValues(String fieldName,
      Object fieldValue) {
    Schema schema = avroSchema.getAvroSchema();
    Field field = schema.getField(fieldName);
    if (field == null) {
      throw new ValidationException("No field named " + fieldName
          + " in schema " + schema);
    }
    if (field.schema().getType() == Schema.Type.MAP) {
      return new HashMap<CharSequence, Object>(
          (Map<CharSequence, Object>) fieldValue);
    } else if (field.schema().getType() == Schema.Type.RECORD) {
      Map<CharSequence, Object> keyAsColumnValues = new HashMap<CharSequence, Object>();
      IndexedRecord avroRecord = (IndexedRecord) fieldValue;
      for (Field avroRecordField : avroRecord.getSchema().getFields()) {
        keyAsColumnValues.put(avroRecordField.name(),
            avroRecord.get(avroRecordField.pos()));
      }
      return keyAsColumnValues;
    } else {
      throw new ValidationException(
          "Only MAP or RECORD type valid for keyAsColumn fields. Found "
              + field.schema().getType());
    }
  }

  @Override
  public Object buildKeyAsColumnField(String fieldName,
      Map<CharSequence, Object> keyAsColumnValues) {
    Schema schema = avroSchema.getAvroSchema();
    Field field = schema.getField(fieldName);
    if (field == null) {
      throw new ValidationException("No field named " + fieldName
          + " in schema " + schema);
    }

    Schema.Type fieldType = field.schema().getType();
    if (fieldType == Schema.Type.MAP) {
      Map<CharSequence, Object> retMap = new HashMap<CharSequence, Object>();
      for (Entry<CharSequence, Object> entry : keyAsColumnValues.entrySet()) {
        retMap.put(entry.getKey(), entry.getValue());
      }
      return retMap;
    } else if (fieldType == Schema.Type.RECORD) {
      AvroRecordBuilder<E> builder = kacRecordBuilderFactories.get(fieldName)
          .getBuilder();
      for (Entry<CharSequence, Object> keyAsColumnEntry : keyAsColumnValues
          .entrySet()) {
        builder.put(keyAsColumnEntry.getKey().toString(),
            keyAsColumnEntry.getValue());
      }
      return builder.build();
    } else {
      throw new ValidationException(
          "Only MAP or RECORD type valid for keyAsColumn fields. Found "
              + fieldType);
    }
  }

  /**
   * Initialize the AvroRecordBuilderFactories for all keyAsColumn mapped fields
   * that are record types. We need to be able to get record builders for these
   * since the records are broken across many columns, and need to be
   * constructed by the composer.
   */
  private void initRecordBuilderFactories() {
    for (FieldMapping fieldMapping : avroSchema.getColumnMappingDescriptor().getFieldMappings()) {
      if (fieldMapping.getMappingType() == MappingType.KEY_AS_COLUMN) {
        String fieldName = fieldMapping.getFieldName();
        Schema fieldSchema = avroSchema.getAvroSchema().getField(fieldName)
            .schema();
        Schema.Type fieldSchemaType = fieldSchema.getType();
        if (fieldSchemaType == Schema.Type.RECORD) {
          AvroRecordBuilderFactory<E> factory = buildAvroRecordBuilderFactory(fieldSchema);
          kacRecordBuilderFactories.put(fieldName, factory);
        }
      }
    }
  }

  /**
   * Build the appropriate AvroRecordBuilderFactory for this instance. Avro has
   * many different record types, of which we support two: Specific and Generic.
   * 
   * @param schema
   *          The Avro schema needed to construct the AvroRecordBuilderFactory.
   * @return The constructed AvroRecordBuilderFactory.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private AvroRecordBuilderFactory<E> buildAvroRecordBuilderFactory(
      Schema schema) {
    if (specific) {
      Class<E> specificClass;
      String className = schema.getFullName();
      try {
        specificClass = (Class<E>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new DatasetException("Could not get Class instance for "
            + className);
      }
      return new SpecificAvroRecordBuilderFactory(specificClass);
    } else {
      return (AvroRecordBuilderFactory<E>) new GenericAvroRecordBuilderFactory(
          schema);
    }
  }
}
