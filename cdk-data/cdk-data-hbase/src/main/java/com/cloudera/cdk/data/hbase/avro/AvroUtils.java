// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

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

import com.cloudera.cdk.data.dao.HBaseCommonException;
import com.cloudera.cdk.data.dao.SerializationException;

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
      throw new HBaseCommonException(
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
      throw new HBaseCommonException("Error reading from input stream", e);
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
      throw new HBaseCommonException(e);
    } catch (SecurityException e) {
      throw new HBaseCommonException(e);
    } catch (IllegalAccessException e) {
      throw new HBaseCommonException(e);
    } catch (NoSuchFieldException e) {
      throw new HBaseCommonException(e);
    }
    return new AvroKeySchema(schemaField, keySchema.getRawSchema());
  }

  public static AvroEntitySchema mergeSpecificStringTypes(
      Class<? extends SpecificRecord> specificClass,
      AvroEntitySchema entitySchema) {
    Schema schemaField;
    try {
      schemaField = (Schema) specificClass.getField("SCHEMA$").get(null);
    } catch (IllegalArgumentException e) {
      throw new HBaseCommonException(e);
    } catch (SecurityException e) {
      throw new HBaseCommonException(e);
    } catch (IllegalAccessException e) {
      throw new HBaseCommonException(e);
    } catch (NoSuchFieldException e) {
      throw new HBaseCommonException(e);
    }
    return new AvroEntitySchema(entitySchema.getTables(), schemaField,
        entitySchema.getRawSchema(), entitySchema.getFieldMappings(),
        entitySchema.isTransactional());
  }
}
