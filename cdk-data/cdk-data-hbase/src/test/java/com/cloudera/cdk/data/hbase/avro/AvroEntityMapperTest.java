// (c) Copyright 2011-2012 Cloudera, Inc.
package com.cloudera.cdk.data.hbase.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.cloudera.cdk.data.hbase.avro.impl.AvroEntityComposer;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroEntitySerDe;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeyEntitySchemaParser;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySchema;
import com.cloudera.cdk.data.hbase.avro.impl.AvroKeySerDe;
import com.cloudera.cdk.data.hbase.avro.impl.AvroUtils;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.cloudera.cdk.data.hbase.BaseEntityMapper;
import com.cloudera.cdk.data.hbase.EntityMapper;
import com.cloudera.cdk.data.dao.KeyEntity;

public class AvroEntityMapperTest {

  private static final AvroKeyEntitySchemaParser schemaParser = new AvroKeyEntitySchemaParser();

  private final String keySchemaString = "{ \"name\": \"key\", \"type\": \"record\", "
      + "\"fields\": [ "
      + "{ \"name\": \"field1\", \"type\": \"int\" }, "
      + "{ \"name\": \"field2\", \"type\": \"int\" } " + "]}";

  private final String schemaString = "{ \"name\": \"test\", \"type\": \"record\", "
      + "\"fields\": [ "
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
    AvroKeySchema keySchema = schemaParser.parseKey(keySchemaString);
    AvroEntitySchema entitySchema = schemaParser.parseEntity(schemaString);

    AvroKeySerDe<GenericRecord> keySerDe = new AvroKeySerDe<GenericRecord>(
        keySchema.getAvroSchema(), false);
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        new AvroEntityComposer<GenericRecord>(entitySchema, false),
        entitySchema, entitySchema, false);
    EntityMapper<GenericRecord, GenericRecord> entityMapper = new BaseEntityMapper<GenericRecord, GenericRecord>(
        keySchema, entitySchema, keySerDe, entitySerDe);

    byte[] row = new byte[] { (byte) 0x80, (byte) 0, (byte) 0, (byte) 1,
        (byte) 0x80, (byte) 0, (byte) 0, (byte) 2 };

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

    KeyEntity<GenericRecord, GenericRecord> keyEntity = entityMapper
        .mapToEntity(result);

    GenericRecord key = keyEntity.getKey();
    GenericRecord entity = keyEntity.getEntity();

    assertEquals(1, key.get("field1"));
    assertEquals(2, key.get("field2"));
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
    AvroKeySchema keySchema = schemaParser.parseKey(keySchemaString);
    AvroEntitySchema entitySchema = schemaParser.parseEntity(schemaString);

    AvroKeySerDe<GenericRecord> keySerDe = new AvroKeySerDe<GenericRecord>(
        keySchema.getAvroSchema(), false);
    AvroEntitySerDe<GenericRecord> entitySerDe = new AvroEntitySerDe<GenericRecord>(
        new AvroEntityComposer<GenericRecord>(entitySchema, false),
        entitySchema, entitySchema, false);
    EntityMapper<GenericRecord, GenericRecord> entityMapper = new BaseEntityMapper<GenericRecord, GenericRecord>(
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

    record.put("field1", 1);
    record.put("field2", 2);
    record.put("field3", map);
    record.put("field4", subRecord);

    GenericRecord key = new GenericData.Record(keySchema.getAvroSchema());
    key.put("field1", 1);
    key.put("field2", 2);

    Put put = entityMapper.mapFromEntity(key, record).getPut();

    GenericRecord putKey = keySerDe.deserialize(put.getRow());
    assertEquals(key.get("field1"), putKey.get("field1"));
    assertEquals(key.get("field2"), putKey.get("field2"));

    List<KeyValue> field1 = put.get(stringToBytes("int"), stringToBytes("1"));
    assertEquals(1, field1.size());
    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 1 },
        field1.get(0).getValue());

    List<KeyValue> field2 = put.get(stringToBytes("int"), stringToBytes("2"));
    assertEquals(1, field2.size());
    assertArrayEquals(new byte[] { (byte) 0, (byte) 0, (byte) 0, (byte) 2 },
        field2.get(0).getValue());

    Map<byte[], List<KeyValue>> famMap = put.getFamilyMap();
    assertArrayEquals(
        concat(new byte[] { (byte) 14 }, stringToBytes("string3")),
        famMap.get(stringToBytes("map")).get(0).getValue());
    assertArrayEquals(
        concat(new byte[] { (byte) 14 }, stringToBytes("string2")),
        famMap.get(stringToBytes("map")).get(1).getValue());
    assertArrayEquals(
        concat(new byte[] { (byte) 14 }, stringToBytes("string1")),
        famMap.get(stringToBytes("map")).get(2).getValue());

    assertArrayEquals(new byte[] { 0x02 }, famMap.get(stringToBytes("record"))
        .get(0).getValue());
    assertArrayEquals(new byte[] { 0x04 }, famMap.get(stringToBytes("record"))
        .get(1).getValue());
  }

  private byte[] stringToBytes(String str) throws UnsupportedEncodingException {
    return str.getBytes("UTF-8");
  }

  private byte[] concat(byte[] A, byte[] B) {
    byte[] C = new byte[A.length + B.length];
    System.arraycopy(A, 0, C, 0, A.length);
    System.arraycopy(B, 0, C, A.length, B.length);
    return C;
  }
}
