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
package com.cloudera.cdk.data.hbase.avro.impl;

import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.cloudera.cdk.data.hbase.avro.io.MemcmpDecoder;
import com.cloudera.cdk.data.hbase.avro.io.MemcmpEncoder;
import com.cloudera.cdk.data.dao.KeyBuildException;
import com.cloudera.cdk.data.hbase.KeySerDe;
import com.cloudera.cdk.data.dao.PartialKey;
import com.cloudera.cdk.data.dao.PartialKey.KeyPartNameValue;

/**
 * Avro implementation of the KeySerDe interface. This will serialize Keys and
 * PartialKeys to a special ordered memcmp-able avro encoding.
 * 
 * @param <K>
 *          The Key type.
 */
public class AvroKeySerDe<K extends IndexedRecord> implements KeySerDe<K> {

  private final Schema schema;
  private final boolean specific;

  public AvroKeySerDe(Schema schema, boolean specific) {
    this.schema = schema;
    this.specific = specific;
  }

  @Override
  public byte[] serialize(K key) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new MemcmpEncoder(outputStream);
    DatumWriter<K> datumWriter = buildDatumWriter(schema);
    AvroUtils.writeAvroEntity(key, encoder, datumWriter);
    return outputStream.toByteArray();
  }

  @Override
  public byte[] serializePartial(PartialKey<K> partialKey) {
    boolean partNotFound = false;
    List<KeyPartNameValue> partsOfKey = new ArrayList<KeyPartNameValue>();
    List<Field> fieldsPartOfKey = new ArrayList<Field>();
    for (Field field : schema.getFields()) {
      KeyPartNameValue part = partialKey.getKeyPartByName(field.name());
      if (part == null) {
        partNotFound = true;
      } else {
        if (partNotFound) {
          throw new KeyBuildException("Key part skipped field in schema.");
        }
        partsOfKey.add(part);
        fieldsPartOfKey.add(AvroUtils.cloneField(field));
      }
    }
    if (partsOfKey.size() != partialKey.getPartList().size()) {
      throw new KeyBuildException(
          "Some parts don't match fields in the schema.");
    }

    Schema partialSchema = Schema.createRecord(fieldsPartOfKey);

    GenericRecord record = new GenericData.Record(schema);
    for (KeyPartNameValue keyPartNameValue : partsOfKey) {
      record.put(keyPartNameValue.getName(), keyPartNameValue.getValue());
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new MemcmpEncoder(outputStream);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
        partialSchema);
    AvroUtils.writeAvroEntity(record, encoder, datumWriter);
    return outputStream.toByteArray();
  }

  @Override
  public K deserialize(byte[] keyBytes) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(keyBytes);
    Decoder decoder = new MemcmpDecoder(inputStream);
    DatumReader<K> datumReader = buildDatumReader(schema);
    return AvroUtils.readAvroEntity(decoder, datumReader);
  }

  private DatumReader<K> buildDatumReader(Schema schema) {
    if (specific) {
      return new SpecificDatumReader<K>(schema);
    } else {
      return new GenericDatumReader<K>(schema);
    }
  }

  private DatumWriter<K> buildDatumWriter(Schema schema) {
    if (specific) {
      return new SpecificDatumWriter<K>(schema);
    } else {
      return new GenericDatumWriter<K>(schema);
    }
  }
}
