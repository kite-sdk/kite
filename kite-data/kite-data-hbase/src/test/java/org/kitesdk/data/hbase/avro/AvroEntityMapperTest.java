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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import org.kitesdk.data.hbase.impl.BaseEntityMapper;
import org.kitesdk.data.hbase.impl.EntityMapper;

public class AvroEntityMapperTest {

  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  private final String schemaString = "{ \"name\": \"test\", \"type\": \"record\", "
      + "\"partitions\": ["
      + "    {\"source\": \"keyPart1\", \"type\": \"hash\", \"buckets\": 4},"
      + "    {\"source\": \"keyPart2\", \"type\": \"identity\"},"
      + "    {\"source\": \"keyPart1\", \"type\": \"identity\"}"
      + "],"
      + "\"fields\": [ "
      + "{ \"name\": \"keyPart1\", \"type\": \"int\", \"mapping\": "
      + "    { \"type\": \"key\" } "
      + "}, "
      + "{ \"name\": \"keyPart2\", \"type\": \"int\",  \"mapping\": "
      + "    { \"type\": \"key\" } "
      + "}, "      
      + "{ \"name\": \"field1\", \"type\": \"int\", \"mapping\": "
      + "    { \"type\": \"column\", \"value\": \"int:1\" } "
      + "}, "
      + "{ \"name\": \"field2\", \"type\": \"int\",  \"mapping\": "
      + "    { \"type\": \"column\", \"value\": \"int:2\" } "
      + "}, "
      + "{ \"name\": \"field3\", "
      + "    \"type\": { \"type\": \"map\", \"values\": \"string\" }, "
      + "    \"mapping\": { \"type\": \"keyAsColumn\", \"value\": \"map\"}"
      + "}, "
      + "{ \"name\": \"field4\", \"type\": { "
      + "    \"type\": \"record\", \"name\": \"test2\", \"fields\": [ "
      + "        { \"name\": \"sub_field1\", \"type\": \"int\" }, "
      + "        { \"name\": \"sub_field2\", \"type\": \"int\" } "
      + "    ]},"
      + "    \"mapping\": { \"type\": \"keyAsColumn\", \"value\": \"record:\"} "
      + "}]}";

  @Test
  public void testMapToEntity() throws Exception {
    AvroKeySchema keySchema = schemaParser.parseKeySchema(schemaString);
    AvroEntitySchema entitySchema = schemaParser.parseEntitySchema(schemaString);

    AvroKeySerDe keySerDe = new AvroKeySerDe(
        keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        new AvroEntityComposer<GenericRecord>(entitySchema, false),
        entitySchema, entitySchema, false);
    EntityMapper<GenericRecord> entityMapper = new BaseEntityMapper<GenericRecord>(
        keySchema, entitySchema, keySerDe, entitySerDe);

    byte[] row = new byte[] { (byte) 0x80, (byte) 0, (byte) 0, (byte) 0, // hash
        (byte) 0x80, (byte) 0, (byte) 0, (byte) 2,   // keyPart2
        (byte) 0x80, (byte) 0, (byte) 0, (byte) 1 }; // keyPart1

    byte[] intFamily = stringToBytes("int");
    byte[] mapFamily = stringToBytes("map");
    byte[] recordFamily = stringToBytes("record");
    byte[] intQual1 = stringToBytes("1");
    byte[] intQual2 = stringToBytes("2");
    byte[] mapQual1 = stringToBytes("1");
    byte[] mapQual2 = stringToBytes("2");
    byte[] mapQual3 = stringToBytes("3");
    byte[] recordQual1 = stringToBytes("sub_field1");
    byte[] recordQual2 = stringToBytes("sub_field2");

    byte[] intValue1 = new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 1 };
    byte[] intValue2 = new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 2 };
    byte[] recordIntValue1 = new byte[] { (byte) 1 };
    byte[] recordIntValue2 = new byte[] { (byte) 2 };
    @SuppressWarnings("deprecation")
    Schema strSchema = Schema.parse("{ \"type\": \"string\" }");
    DatumWriter<Utf8> datumWriter = new GenericDatumWriter<Utf8>(strSchema);
    byte[] stringValue1 = AvroUtils.writeAvroEntity(new Utf8("string_value1"),
        datumWriter);
    byte[] stringValue2 = AvroUtils.writeAvroEntity(new Utf8("string_value2"),
        datumWriter);
    byte[] stringValue3 = AvroUtils.writeAvroEntity(new Utf8("string_value3"),
        datumWriter);

    KeyValue[] keyValues = new KeyValue[] {
        new KeyValue(row, intFamily, intQual1, intValue1),
        new KeyValue(row, intFamily, intQual2, intValue2),
        new KeyValue(row, mapFamily, mapQual1, stringValue1),
        new KeyValue(row, mapFamily, mapQual2, stringValue2),
        new KeyValue(row, mapFamily, mapQual3, stringValue3),
        new KeyValue(row, recordFamily, recordQual1, recordIntValue1),
        new KeyValue(row, recordFamily, recordQual2, recordIntValue2) };
    Result result = new Result(keyValues);

    GenericRecord entity = entityMapper
        .mapToEntity(result);

    assertEquals(1, entity.get("keyPart1"));
    assertEquals(2, entity.get("keyPart2"));
    assertEquals(1, entity.get("field1"));
    assertEquals(2, entity.get("field2"));

    @SuppressWarnings("unchecked")
    Map<CharSequence, Utf8> field3 = (Map<CharSequence, Utf8>) entity
        .get("field3");
    assertEquals("string_value1", field3.get(new Utf8("1")).toString());
    assertEquals("string_value2", field3.get(new Utf8("2")).toString());
    assertEquals("string_value3", field3.get(new Utf8("3")).toString());

    GenericRecord field4 = (GenericRecord) entity.get("field4");
    assertEquals(-1, ((Integer) field4.get("sub_field1")).intValue());
    assertEquals(1, ((Integer) field4.get("sub_field2")).intValue());
  }

  @Test
  public void testMapFromEntity() throws Exception {
    AvroKeySchema keySchema = schemaParser.parseKeySchema(schemaString);
    AvroEntitySchema entitySchema = schemaParser.parseEntitySchema(schemaString);

    AvroKeySerDe keySerDe = new AvroKeySerDe(
        keySchema.getAvroSchema(), keySchema.getPartitionStrategy());
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        new AvroEntityComposer<GenericRecord>(entitySchema, false),
        entitySchema, entitySchema, false);
    EntityMapper<GenericRecord> entityMapper = new BaseEntityMapper<GenericRecord>(
        keySchema, entitySchema, keySerDe, entitySerDe);
    @SuppressWarnings("deprecation")
    GenericRecord record = new GenericData.Record(Schema.parse(schemaString));

    @SuppressWarnings("deprecation")
    Schema subRecordSchema = Schema.parse(schemaString).getField("field4")
        .schema();
    GenericRecord subRecord = new GenericData.Record(subRecordSchema);

    Map<String, Utf8> map = new HashMap<String, Utf8>();
    map.put("1", new Utf8("string1"));
    map.put("2", new Utf8("string2"));
    map.put("3", new Utf8("string3"));
    subRecord.put("sub_field1", 1);
    subRecord.put("sub_field2", 2);

    record.put("keyPart1", 1);
    record.put("keyPart2", 2);
    record.put("field1", 1);
    record.put("field2", 2);
    record.put("field3", map);
    record.put("field4", subRecord);
    
    Put put = entityMapper.mapFromEntity(record).getPut();

    // Careful to support both KeyValue (from HBase 0.94) and Cell (from 0.96) in the
    // rest of this method
    List field1 = put.get(stringToBytes("int"), stringToBytes("1"));
    assertEquals(1, field1.size());
    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 1 },
        ((KeyValue) field1.get(0)).getValue());

    List field2 = put.get(stringToBytes("int"), stringToBytes("2"));
    assertEquals(1, field2.size());
    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 2 },
        ((KeyValue) field2.get(0)).getValue());

    Map<byte[],List<KeyValue>> famMap = put.getFamilyMap();
    assertKeyValuesMatchMap(ImmutableMap.of(
            "1", concat(new byte[] {(byte)14}, stringToBytes(map.get("1"))),
            "2", concat(new byte[] {(byte)14}, stringToBytes(map.get("2"))),
            "3", concat(new byte[] {(byte)14}, stringToBytes(map.get("3")))),
        famMap.get(stringToBytes("map")));

    assertKeyValuesMatchMap(ImmutableMap.of(
            "sub_field1", new byte[] {0x02},
            "sub_field2", new byte[] {0x04}),
        famMap.get(stringToBytes("record")));
  }

  private void assertKeyValuesMatchMap(Map<String, byte[]> expected, List<KeyValue> kvs)
      throws UnsupportedEncodingException {
    Set<String> keys = Sets.newHashSet();
    for (KeyValue kv : kvs) {
      String key = bytesToString(kv.getBuffer(),
          kv.getQualifierOffset(), kv.getQualifierLength());

      assertArrayEquals(expected.get(key), kv.getValue());
      keys.add(key);
    }
    assertEquals(keys, expected.keySet());
  }

  private byte[] stringToBytes(String str) throws UnsupportedEncodingException {
    return str.getBytes("UTF-8");
  }

  private byte[] stringToBytes(Utf8 str) {
    return Arrays.copyOf(str.getBytes(), str.getByteLength());
  }

  private String bytesToString(byte[] bytes, int offset, int len) throws UnsupportedEncodingException {
    return new String(bytes, offset, len, "UTF-8");
  }

  private byte[] concat(byte[] A, byte[] B) {
    byte[] C = new byte[A.length + B.length];
    System.arraycopy(A, 0, C, 0, A.length);
    System.arraycopy(B, 0, C, A.length, B.length);
    return C;
  }
}
