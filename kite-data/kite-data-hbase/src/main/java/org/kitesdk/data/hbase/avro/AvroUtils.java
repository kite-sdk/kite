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

import org.kitesdk.data.DatasetException;
import org.kitesdk.data.SerializationException;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.JsonNode;

/**
 * Utility functions for Avro instances.
 */
public class AvroUtils {

  /**
   * Given a byte array and a DatumReader, decode an avro entity from the byte
   * array. Decodes using the avro BinaryDecoder. Return the constructed entity.
   * 
   * @param bytes
   *          The byte array to decode the entity from.
   * @param reader
   *          The DatumReader that will decode the byte array.
   * @return The Avro entity.
   */
  public static <T> T readAvroEntity(byte[] bytes, DatumReader<T> reader) {
    Decoder decoder = new DecoderFactory().binaryDecoder(bytes, null);
    return AvroUtils.<T> readAvroEntity(decoder, reader);
  }

  /**
   * Decode an entity from the initialized Avro Decoder using the DatumReader.
   * 
   * @param decoder
   *          The decoder to decode the entity fields
   * @param reader
   *          The Avro DatumReader that will read the entity with the decoder.
   * @return The entity.
   */
  public static <T> T readAvroEntity(Decoder decoder, DatumReader<T> reader) {
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new SerializationException("Could not deserialize Avro entity", e);
    }
  }

  /**
   * Given an entity and a DatumReader, encode the avro entity to a byte array.
   * Encodes using the avro BinaryEncoder. Return the serialized bytes.
   * 
   * @param entity
   *          The entity we want to encode.
   * @param writer
   *          The DatumWriter we'll use to encode the entity to a byte array
   * @return The avro entity encoded in a byte array.
   */
  public static <T> byte[] writeAvroEntity(T entity, DatumWriter<T> writer) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new EncoderFactory().binaryEncoder(outputStream, null);
    writeAvroEntity(entity, encoder, writer);
    return outputStream.toByteArray();
  }

  /**
   * Given an entity, an avro schema, and an encoder, write the entity to the
   * encoder's underlying output stream.
   * 
   * @param entity
   *          The entity we want to encode.
   * @param encoder
   *          The Avro Encoder we will write to.
   * @param writer
   *          The DatumWriter we'll use to encode the entity to the encoder.
   */
  public static <T> void writeAvroEntity(T entity, Encoder encoder,
      DatumWriter<T> writer) {
    try {
      writer.write(entity, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new SerializationException("Could not serialize Avro entity", e);
    }
  }

  /**
   * Given an avro Schema.Field instance, make a clone of it.
   * 
   * @param field
   *          The field to clone.
   * @return The cloned field.
   */
  public static Field cloneField(Field field) {
    return new Field(field.name(), field.schema(), field.doc(),
        field.defaultValue());
  }

  /**
   * Convert an InputStream to a string encoded as UTF-8.
   * 
   * @param in
   *          The InputStream to read the schema from.
   * @return The string.
   */
  public static String inputStreamToString(InputStream in) {
    final int BUFFER_SIZE = 1024;
    BufferedReader bufferedReader;
    try {
      bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new DatasetException(
          "Platform doesn't support UTF-8. It must!", e);
    }
    char[] buffer = new char[BUFFER_SIZE];
    StringBuilder stringBuilder = new StringBuilder(BUFFER_SIZE);
    int bytesRead = 0;
    try {
      while ((bytesRead = bufferedReader.read(buffer, 0, BUFFER_SIZE)) > 0) {
        stringBuilder.append(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      throw new DatasetException("Error reading from input stream", e);
    }
    return stringBuilder.toString();
  }

  /**
   * Get a map of field names to default values for an Avro schema.
   * 
   * @param avroRecordSchema
   *          The schema to get the map of field names to values.
   * @return The map.
   */
  public static Map<String, Object> getDefaultValueMap(Schema avroRecordSchema) {
    List<Field> defaultFields = new ArrayList<Field>();
    for (Field f : avroRecordSchema.getFields()) {
      if (f.defaultValue() != null) {
        // Need to create a new Field here or we will get
        // org.apache.avro.AvroRuntimeException: Field already used:
        // schemaVersion
        defaultFields.add(new Field(f.name(), f.schema(), f.doc(), f
            .defaultValue(), f.order()));
      }
    }

    Schema defaultSchema = Schema.createRecord(defaultFields);
    Schema emptyRecordSchema = Schema.createRecord(new ArrayList<Field>());
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
        emptyRecordSchema);
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
        emptyRecordSchema, defaultSchema);

    GenericRecord emptyRecord = new GenericData.Record(emptyRecordSchema);
    GenericRecord defaultRecord = AvroUtils.readAvroEntity(
        AvroUtils.writeAvroEntity(emptyRecord, writer), reader);

    Map<String, Object> defaultValueMap = new HashMap<String, Object>();
    for (Field f : defaultFields) {
      defaultValueMap.put(f.name(), defaultRecord.get(f.name()));
    }
    return defaultValueMap;
  }

  public static AvroKeySchema mergeSpecificStringTypes(
      Class<? extends SpecificRecord> specificClass, AvroKeySchema keySchema) {
    Schema schemaField;
    try {
      schemaField = (Schema) specificClass.getField("SCHEMA$").get(null);
    } catch (IllegalArgumentException e) {
      throw new DatasetException(e);
    } catch (SecurityException e) {
      throw new DatasetException(e);
    } catch (IllegalAccessException e) {
      throw new DatasetException(e);
    } catch (NoSuchFieldException e) {
      throw new DatasetException(e);
    }
    // Ensure schema is limited to keySchema's fields. The class may have more
    // fields
    // in the case that the entity is being used as a key.
    List<Field> fields = Lists.newArrayList();
    for (Schema.Field field : keySchema.getAvroSchema().getFields()) {
      fields.add(copy(schemaField.getField(field.name())));
    }
    Schema schema = Schema.createRecord(keySchema.getAvroSchema().getName(),
        keySchema.getAvroSchema().getDoc(), keySchema.getAvroSchema()
            .getNamespace(), keySchema.getAvroSchema().isError());
    schema.setFields(fields);
    return new AvroKeySchema(schema, keySchema.getRawSchema(),
        keySchema.getPartitionStrategy());
  }

  private static Schema.Field copy(Schema.Field f) {
    Schema.Field copy = AvroUtils.cloneField(f);
    // retain mapping properties
    for (Map.Entry<String, JsonNode> prop : f.getJsonProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }
    return copy;
  }

  public static AvroEntitySchema mergeSpecificStringTypes(
      Class<? extends SpecificRecord> specificClass,
      AvroEntitySchema entitySchema) {
    Schema schemaField;
    try {
      schemaField = (Schema) specificClass.getField("SCHEMA$").get(null);
    } catch (IllegalArgumentException e) {
      throw new DatasetException(e);
    } catch (SecurityException e) {
      throw new DatasetException(e);
    } catch (IllegalAccessException e) {
      throw new DatasetException(e);
    } catch (NoSuchFieldException e) {
      throw new DatasetException(e);
    }
    return new AvroEntitySchema(entitySchema.getTables(), schemaField,
        entitySchema.getRawSchema(), entitySchema.getFieldMappings());
  }

  /**
   * Returns true if the types of two avro schemas are equal. This ignores
   * things like custom field properties that the equals() implementation of
   * Schema checks.
   * 
   * @param schema1
   *          The first schema to compare
   * @param schema2
   *          The second schema to compare
   * @return True if the types are equal, otherwise false.
   */
  public static boolean avroSchemaTypesEqual(Schema schema1, Schema schema2) {
    if (schema1.getType() != schema2.getType()) {
      // if the types aren't equal, no need to go further. Return false
      return false;
    }

    if (schema1.getType() == Schema.Type.ENUM
        || schema1.getType() == Schema.Type.FIXED) {
      // Enum and Fixed types schemas should be equal using the Schema.equals
      // method.
      return schema1.equals(schema2);
    }
    if (schema1.getType() == Schema.Type.ARRAY) {
      // Avro element schemas should be equal, which is tested by recursively
      // calling this method.
      return avroSchemaTypesEqual(schema1.getElementType(),
          schema2.getElementType());
    } else if (schema1.getType() == Schema.Type.MAP) {
      // Map type values schemas should be equal, which is tested by recursively
      // calling this method.
      return avroSchemaTypesEqual(schema1.getValueType(),
          schema2.getValueType());
    } else if (schema1.getType() == Schema.Type.UNION) {
      // Compare Union fields in the same position by comparing their schemas
      // recursively calling this method.
      if (schema1.getTypes().size() != schema2.getTypes().size()) {
        return false;
      }
      for (int i = 0; i < schema1.getTypes().size(); i++) {
        if (!avroSchemaTypesEqual(schema1.getTypes().get(i), schema2.getTypes()
            .get(i))) {
          return false;
        }
      }
      return true;
    } else if (schema1.getType() == Schema.Type.RECORD) {
      // Compare record fields that match in name by comparing their schemas
      // recursively calling this method.
      if (schema1.getFields().size() != schema2.getFields().size()) {
        return false;
      }
      for (Field field1 : schema1.getFields()) {
        Field field2 = schema2.getField(field1.name());
        if (field2 == null) {
          return false;
        }
        if (!avroSchemaTypesEqual(field1.schema(), field2.schema())) {
          return false;
        }
      }
      return true;
    } else {
      // All other types are primitive, so them matching in type is enough.
      return true;
    }
  }
}
