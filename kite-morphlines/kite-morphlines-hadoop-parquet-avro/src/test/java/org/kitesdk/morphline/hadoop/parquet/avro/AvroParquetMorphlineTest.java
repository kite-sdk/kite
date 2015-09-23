/*
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
package org.kitesdk.morphline.hadoop.parquet.avro;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.hadoop.parquet.avro.ReadAvroParquetFileBuilder;

import org.apache.parquet.avro.AvroParquetWriter;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

public class AvroParquetMorphlineTest extends AbstractMorphlineTest {

  @Test
  public void testMapWithUtf8Key() throws Exception {
    Schema schema = new Schema.Parser().parse(new File("src/test/resources/test-avro-schemas/map.avsc"));

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    AvroParquetWriter<GenericRecord> writer = 
        new AvroParquetWriter<GenericRecord>(file, schema);

    // Write a record with a map with Utf8 keys.
    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", new HashMap(ImmutableMap.of(utf8("a"), 1, utf8("b"), 2)))
        .build();
    writer.write(record);
    writer.close();

    for (String configFile : Arrays.asList(
        "readAvroParquetFile", 
        "readAvroParquetFileWithProjectionSchema", 
        "readAvroParquetFileWithReaderSchema1",
        "readAvroParquetFileWithReaderSchemaExternal"
        )) {
      morphline = createMorphline("test-morphlines/" + configFile);
      
      Record morphlineRecord = new Record();
      morphlineRecord.put(ReadAvroParquetFileBuilder.FILE_UPLOAD_URL, file.toString());
      collector.reset();
      
      assertTrue(morphline.process(morphlineRecord));

      assertEquals(1, collector.getRecords().size());
      GenericData.Record actualRecord = (GenericData.Record) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
      assertEquals(record, actualRecord);      
    }
  }
  
  @Test
  public void testAll() throws Exception {
    Schema schema = new Schema.Parser().parse(new File("src/test/resources/test-avro-schemas/all.avsc"));

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());
    
    AvroParquetWriter<GenericRecord> writer = new
        AvroParquetWriter<GenericRecord>(file, schema);

    GenericData.Record nestedRecord = new GenericRecordBuilder(
        schema.getField("mynestedrecord").schema())
            .set("mynestedint", 1).build();

    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    GenericData.Array<Integer> genericIntegerArray = new GenericData.Array<Integer>(
        Schema.createArray(Schema.create(Schema.Type.INT)), integerArray);

    GenericFixed genericFixed = new GenericData.Fixed(
        Schema.createFixed("fixed", null, null, 1), new byte[] { (byte) 65 });

    List<Integer> emptyArray = new ArrayList<Integer>();
    ImmutableMap emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mynull", null)
        .set("myboolean", true)
        .set("myint", 1)
        .set("mylong", 2L)
        .set("myfloat", 3.1f)
        .set("mydouble", 4.1)
        .set("mybytes", ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)))
        .set("mystring", "hello")
        .set("mynestedrecord", nestedRecord)
        .set("myenum", "a")
        .set("myarray", genericIntegerArray)
        .set("myemptyarray", emptyArray)
        .set("myoptionalarray", genericIntegerArray)
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
        .set("myemptymap", emptyMap)
        .set("myfixed", genericFixed)
        .build();

    writer.write(record);
    writer.close();

    morphline = createMorphline("test-morphlines/readAvroParquetFileWithProjectionSubSchema");
    
    Record morphlineRecord = new Record();
    morphlineRecord.put(ReadAvroParquetFileBuilder.FILE_UPLOAD_URL, file.toString());
    collector.reset();
    
    assertTrue(morphline.process(morphlineRecord));

    assertEquals(1, collector.getRecords().size());
    GenericData.Record actualRecord = (GenericData.Record) collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_BODY);
    assertNotNull(actualRecord);
    assertEquals(null, actualRecord.get("mynull"));
    assertEquals(true, actualRecord.get("myboolean"));
    assertEquals(1, actualRecord.get("myint"));
    assertEquals(2L, actualRecord.get("mylong"));
    assertEquals(null, actualRecord.get("myfloat"));
    assertEquals(4.1, actualRecord.get("mydouble"));
    assertEquals(ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)), actualRecord.get("mybytes"));
    assertEquals("hello", actualRecord.get("mystring"));
    assertEquals("a", actualRecord.get("myenum"));
    assertEquals(nestedRecord, actualRecord.get("mynestedrecord"));
    assertEquals(integerArray, actualRecord.get("myarray"));
    assertEquals(emptyArray, actualRecord.get("myemptyarray"));
    assertEquals(integerArray, actualRecord.get("myoptionalarray"));
    assertEquals(ImmutableMap.of("a", 1, "b", 2), actualRecord.get("mymap"));
    assertEquals(emptyMap, actualRecord.get("myemptymap"));
    assertEquals(genericFixed, actualRecord.get("myfixed"));
  }

  private static String utf8(String str) {
    return str;
  }

//private static Utf8 utf8(String str) {
//return new Utf8(str);
//}

}
