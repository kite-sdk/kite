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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.FieldMapping.MappingType;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.hbase.avro.io.ColumnDecoder;
import org.kitesdk.data.hbase.avro.io.ColumnEncoder;
import org.kitesdk.data.hbase.impl.EntityComposer;
import org.kitesdk.data.hbase.impl.EntitySerDe;

/**
 * An EntitySerDe implementation that serializes and deserializes Avro records.
 * 
 * @param <E>
 *          The type of entity this SerDe works with.
 */
public class AvroEntitySerDe<E extends IndexedRecord> extends EntitySerDe<E> {

  /**
   * Boolean to indicate whether this is a specific record or generic record
   * SerDe. TODO: Eventually use an enum type when we support more than two
   * types of Avro records.
   */
  private final boolean specific;

  /**
   * The Avro schema for the Avro records this EntitySerDe will serialize and
   * deserialize.
   */
  private final AvroEntitySchema avroSchema;

  /**
   * A mapping of Avro entity fields to their DatumReaders
   */
  private final Map<String, DatumReader<Object>> fieldDatumReaders = new HashMap<String, DatumReader<Object>>();

  /**
   * A mapping of Avro entity field names to their DatumWriters
   */
  private final Map<String, DatumWriter<Object>> fieldDatumWriters = new HashMap<String, DatumWriter<Object>>();

  /**
   * DatumReaders for keyAsColumn Avro Record fields. The inner map maps from
   * the keyAsColumn Record's fields to each DatumReader. The outer map maps
   * from the Avro entity's field to the inner map.
   */
  private final Map<String, Map<String, DatumReader<Object>>> kacRecordDatumReaders = new HashMap<String, Map<String, DatumReader<Object>>>();

  /**
   * DatumWriters for keyAsColumn Avro Record fields. The inner map maps from
   * the keyAsColumn Record's fields to each DatumWriter. The outer map maps
   * from the Avro entity's field to the inner map.
   */
  private final Map<String, Map<String, DatumWriter<Object>>> kacRecordDatumWriters = new HashMap<String, Map<String, DatumWriter<Object>>>();

  /**
   * A mapping of field default values for fields that have them
   */
  private final Map<String, Object> defaultValueMap;

  /**
   * Constructor for AvroEntitySerDe instances.
   * 
   * @param entityComposer
   *          An entity composer that can construct Avro entities
   * @param avroSchema
   *          The avro schema for entities this SerDe serializes and
   *          deserializes
   * @param writtenAvroSchema
   *          The avro schema a record we are reading was written with
   * @param specific
   *          True if the entity is a Specific avro record. False indicates it's
   *          a generic
   */
  public AvroEntitySerDe(EntityComposer<E> entityComposer,
      AvroEntitySchema avroSchema, AvroEntitySchema writtenAvroSchema,
      boolean specific) {
    super(entityComposer);
    this.specific = specific;
    this.avroSchema = avroSchema;
    this.defaultValueMap = AvroUtils.getDefaultValueMap(avroSchema.getAvroSchema());

    // For each field in entity, initialize the appropriate datum readers and
    // writers.
    for (FieldMapping fieldMapping : avroSchema.getColumnMappingDescriptor().getFieldMappings()) {
      String fieldName = fieldMapping.getFieldName();
      Schema fieldSchema = avroSchema.getAvroSchema().getField(fieldName)
          .schema();
      Field writtenField = writtenAvroSchema.getAvroSchema()
          .getField(fieldName);
      if (writtenField == null) {
        // No field for the written version, so don't worry about datum
        // readers and writers.
        continue;
      }
      Schema writtenFieldSchema = writtenField.schema();

      if (fieldMapping.getMappingType() == MappingType.COLUMN
          || fieldMapping.getMappingType() == MappingType.COUNTER) {
        initColumnDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
      } else if (fieldMapping.getMappingType() == MappingType.KEY_AS_COLUMN) {
        if (fieldSchema.getType() == Schema.Type.RECORD) {
          // Each field of the kac record has a different type, so we need
          // to track each one in a different map.
          initKACRecordDatumMaps(fieldName, fieldSchema, writtenFieldSchema);
        } else if (fieldSchema.getType() == Schema.Type.MAP) {
          // Only one value type for a map, so just put the type in the column
          // datum maps.
          initColumnDatumMaps(fieldName, fieldSchema.getValueType(),
              writtenFieldSchema.getValueType());
        } else {
          throw new ValidationException(
              "Unsupported type for keyAsColumn: " + fieldSchema.getType());
        }
      }
    }
  }

  @Override
  public byte[] serializeColumnValueToBytes(String fieldName, Object columnValue) {
    Field field = avroSchema.getAvroSchema().getField(fieldName);
    DatumWriter<Object> datumWriter = fieldDatumWriters.get(fieldName);
    if (field == null) {
      throw new ValidationException("Invalid field name " + fieldName
          + " for schema " + avroSchema.toString());
    }
    if (datumWriter == null) {
      throw new ValidationException("No datum writer for field name: "
          + fieldName);
    }

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    Encoder encoder = getColumnEncoder(field.schema(), byteOut);
    AvroUtils.writeAvroEntity(columnValue, encoder,
        fieldDatumWriters.get(fieldName));
    return byteOut.toByteArray();
  }

  @Override
  public byte[] serializeKeyAsColumnValueToBytes(String fieldName,
      CharSequence columnKey, Object columnValue) {
    Field field = avroSchema.getAvroSchema().getField(fieldName);
    if (field == null) {
      throw new ValidationException("Invalid field name " + fieldName
          + " for schema " + avroSchema.toString());
    }

    Schema.Type schemaType = field.schema().getType();
    if (schemaType == Schema.Type.MAP) {
      DatumWriter<Object> datumWriter = fieldDatumWriters.get(fieldName);
      if (datumWriter == null) {
        throw new ValidationException("No datum writer for field name: "
            + fieldName);
      }
      return AvroUtils.writeAvroEntity(columnValue, datumWriter);
    } else if (schemaType == Schema.Type.RECORD) {
      if (!kacRecordDatumWriters.containsKey(fieldName)) {
        throw new ValidationException("Invalid field name " + fieldName
            + " for schema " + avroSchema.toString());
      }
      if (!kacRecordDatumWriters.get(fieldName).containsKey(
          columnKey.toString())) {
        throw new ValidationException("Invalid key in record: "
            + fieldName + "." + columnKey);
      }
      DatumWriter<Object> datumWriter = kacRecordDatumWriters.get(fieldName)
          .get(columnKey.toString());
      return AvroUtils.writeAvroEntity(columnValue, datumWriter);
    } else {
      throw new ValidationException("Unsupported type for keyAsColumn: "
          + schemaType);
    }
  }

  @Override
  public byte[] serializeKeyAsColumnKeyToBytes(String fieldName,
      CharSequence columnKey) {
    if (columnKey.getClass().isAssignableFrom(String.class)) {
      return ((String) columnKey).getBytes();
    } else if (columnKey.getClass().isAssignableFrom(Utf8.class)) {
      return ((Utf8) columnKey).getBytes();
    } else {
      return columnKey.toString().getBytes();
    }
  }

  @Override
  public Object deserializeColumnValueFromBytes(String fieldName, byte[] bytes) {
    Field field = avroSchema.getAvroSchema().getField(fieldName);
    DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);
    if (field == null) {
      throw new ValidationException("Invalid field name " + fieldName
          + " for schema " + avroSchema.toString());
    }
    if (datumReader == null) {
      throw new ValidationException("No datum reader for field name: "
          + fieldName);
    }

    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    Decoder decoder = getColumnDecoder(field.schema(), byteIn);
    return AvroUtils.readAvroEntity(decoder, datumReader);
  }

  @Override
  public Object deserializeKeyAsColumnValueFromBytes(String fieldName,
      byte[] columnKeyBytes, byte[] columnValueBytes) {
    Field field = avroSchema.getAvroSchema().getField(fieldName);
    if (field == null) {
      throw new ValidationException("Invalid field name " + fieldName
          + " for schema " + avroSchema.toString());
    }

    Schema.Type schemaType = field.schema().getType();
    if (schemaType == Schema.Type.MAP) {
      DatumReader<Object> datumReader = fieldDatumReaders.get(fieldName);
      if (datumReader == null) {
        throw new ValidationException("No datum reader for field name: "
            + fieldName);
      }
      return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
    } else if (schemaType == Schema.Type.RECORD) {
      if (!kacRecordDatumReaders.containsKey(fieldName)) {
        throw new ValidationException("Invalid field name " + fieldName
            + " for schema " + avroSchema.toString());
      }
      String columnKey = new String(columnKeyBytes);
      if (!kacRecordDatumReaders.get(fieldName).containsKey(columnKey)) {
        throw new ValidationException("Invalid key in record: "
            + fieldName + "." + columnKey);
      }
      DatumReader<Object> datumReader = kacRecordDatumReaders.get(fieldName)
          .get(columnKey);
      return AvroUtils.readAvroEntity(columnValueBytes, datumReader);
    } else {
      throw new ValidationException("Unsupported type for keyAsColumn: "
          + schemaType);
    }
  }

  @Override
  public CharSequence deserializeKeyAsColumnKeyFromBytes(String fieldName,
      byte[] columnKeyBytes) {
    Field field = avroSchema.getAvroSchema().getField(fieldName);
    if (field == null) {
      throw new ValidationException("Invalid field name " + fieldName
          + " for schema " + avroSchema.toString());
    }

    Schema.Type schemaType = field.schema().getType();
    if (schemaType == Schema.Type.MAP) {
      String stringProp = field.schema().getProp("avro.java.string");
      if (stringProp != null && stringProp.equals("String")) {
        return new String(columnKeyBytes);
      } else {
        return new Utf8(columnKeyBytes);
      }
    } else if (schemaType == Schema.Type.RECORD) {
      return new String(columnKeyBytes);
    } else {
      throw new ValidationException("Unsupported type for keyAsColumn: "
          + schemaType);
    }
  }

  @Override
  public Object getDefaultValue(String fieldName) {
    return defaultValueMap.get(fieldName);
  }

  private void initColumnDatumMaps(String fieldName, Schema fieldSchema,
      Schema writtenFieldSchema) {
    fieldDatumReaders.put(fieldName,
        buildDatumReader(fieldSchema, writtenFieldSchema));
    fieldDatumWriters.put(fieldName, buildDatumWriter(fieldSchema));
  }

  private void initKACRecordDatumMaps(String fieldName, Schema fieldSchema,
      Schema writtenFieldSchema) {
    Map<String, DatumReader<Object>> recordFieldReaderMap = new HashMap<String, DatumReader<Object>>();
    Map<String, DatumWriter<Object>> recordFieldWriterMap = new HashMap<String, DatumWriter<Object>>();
    kacRecordDatumReaders.put(fieldName, recordFieldReaderMap);
    kacRecordDatumWriters.put(fieldName, recordFieldWriterMap);
    for (Field recordField : fieldSchema.getFields()) {
      Field writtenRecordField = writtenFieldSchema
          .getField(recordField.name());
      if (writtenRecordField == null) {
        continue;
      }
      recordFieldReaderMap.put(recordField.name(),
          buildDatumReader(recordField.schema(), writtenRecordField.schema()));
      recordFieldWriterMap.put(recordField.name(),
          buildDatumWriter(recordField.schema()));
    }
  }

  private DatumReader<Object> buildDatumReader(Schema schema,
      Schema writtenSchema) {
    if (specific) {
      return new SpecificDatumReader<Object>(writtenSchema, schema);
    } else {
      return new GenericDatumReader<Object>(writtenSchema, schema);
    }
  }

  private DatumWriter<Object> buildDatumWriter(Schema schema) {
    if (specific) {
      return new SpecificDatumWriter<Object>(schema);
    } else {
      return new GenericDatumWriter<Object>(schema);
    }
  }

  /**
   * Returns an Avro Decoder. The implementation it chooses will depend on the
   * schema of the field.
   * 
   * @param in
   *          InputStream to decode bytes from
   * @return The avro decoder.
   */
  private Decoder getColumnDecoder(Schema writtenFieldAvroSchema, InputStream in) {
    // Use a special Avro decoder that has special handling for int, long,
    // and String types. See ColumnDecoder for more information.
    if (writtenFieldAvroSchema.getType() == Type.INT
        || writtenFieldAvroSchema.getType() == Type.LONG
        || writtenFieldAvroSchema.getType() == Type.STRING) {
      return new ColumnDecoder(in);
    } else {
      return DecoderFactory.get().binaryDecoder(in, null);
    }
  }

  /**
   * Returns an Avro Encoder. The implementation it chooses will depend on the
   * schema of the field.
   * 
   * @param out
   *          Output stream to encode bytes to
   * @return The avro encoder
   */
  private Encoder getColumnEncoder(Schema fieldAvroSchema, OutputStream out) {
    // Use a special Avro encoder that has special handling for int, long,
    // and String types. See ColumnEncoder for more information.
    if (fieldAvroSchema.getType() == Type.INT
        || fieldAvroSchema.getType() == Type.LONG
        || fieldAvroSchema.getType() == Type.STRING) {
      return new ColumnEncoder(out);
    } else {
      return EncoderFactory.get().binaryEncoder(out, null);
    }
  }

}
