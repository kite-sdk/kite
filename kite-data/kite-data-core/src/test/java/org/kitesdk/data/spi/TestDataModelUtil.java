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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import org.kitesdk.data.event.ReflectSmallEvent;
import org.kitesdk.data.event.ReflectStandardEvent;
import org.kitesdk.data.event.SmallEvent;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.spi.filesystem.TestGenericRecord;

public class TestDataModelUtil {
  
  @Test
  public void testDataModelForGenericType() {
    Class<GenericData.Record> type = GenericData.Record.class;
    GenericData result = DataModelUtil.getDataModelForType(type);
    assertEquals(GenericData.class, result.getClass());
  }

  @Test
  public void testDataModelForSpecificType() {
    Class<StandardEvent> type = StandardEvent.class;
    GenericData result = DataModelUtil.getDataModelForType(type);
    assertEquals(SpecificData.class, result.getClass());
  }

  @Test
  public void testDataModelForReflectType() {
    Class<String> type = String.class;
    GenericData result = DataModelUtil.getDataModelForType(type);
    assertEquals(DataModelUtil.AllowNulls.class, result.getClass());
  }

  @Test
  public void testGetDatumReaderForGenericType() {
    Class<GenericData.Record> type = GenericData.Record.class;
    Schema writerSchema = StandardEvent.getClassSchema();
    DatumReader result = DataModelUtil.getDatumReaderForType(type, writerSchema);
    assertEquals(GenericDatumReader.class, result.getClass());
  }

  @Test
  public void testGetDatumReaderForSpecificType() {
    Class<StandardEvent> type = StandardEvent.class;
    Schema writerSchema = StandardEvent.getClassSchema();
    DatumReader result = DataModelUtil.getDatumReaderForType(type, writerSchema);
    assertEquals(SpecificDatumReader.class, result.getClass());
  }

  @Test
  public void testGetDatumReaderForReflectType() {
    Class<String> type = String.class;
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    DatumReader result = DataModelUtil.getDatumReaderForType(type, writerSchema);
    assertEquals(ReflectDatumReader.class, result.getClass());
  }

  @Test
  public void testResolveTypeGenericToGeneric() {
    Class<GenericData.Record> type = GenericData.Record.class;
    Schema schema = StandardEvent.getClassSchema();
    Class expResult = type;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testResolveTypeSpecifcToSpecifc() {
    Class<StandardEvent> type = StandardEvent.class;
    Schema schema = StandardEvent.getClassSchema();
    Class expResult = type;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testResolveTypeReflectToReflect() {
    Class<String> type = String.class;
    Schema schema = Schema.create(Schema.Type.STRING);
    Class expResult = type;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testResolveTypeObjectToSpecifc() {
    Class<Object> type = Object.class;
    Schema schema = StandardEvent.getClassSchema();
    Class expResult = StandardEvent.class;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testResolveTypeObjectToGeneric() {
    Class<Object> type = Object.class;
    Schema schema = SchemaBuilder.record("User").fields()
        .requiredString("name")
        .requiredString("color")
        .endRecord();
    Class expResult = GenericData.Record.class;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testResolveTypeObjectToReflect() {
    Class<Object> type = Object.class;
    Schema schema = ReflectData.get().getSchema(String.class);
    Class expResult = String.class;
    Class result = DataModelUtil.resolveType(type, schema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReaderSchemaForGeneric() {
    Class<GenericData.Record> type = GenericData.Record.class;
    Schema writerSchema = StandardEvent.getClassSchema();
    Schema expResult = writerSchema;
    Schema result = DataModelUtil.getReaderSchema(type, writerSchema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReaderSchemaForSpecific() {
    Class<StandardEvent> type = StandardEvent.class;
    Schema writerSchema = StandardEvent.getClassSchema();
    Schema expResult = writerSchema;
    Schema result = DataModelUtil.getReaderSchema(type, writerSchema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReaderSchemaForReflect() {
    Class<String> type = String.class;
    Schema writerSchema = ReflectData.get().getSchema(String.class);
    Schema expResult = writerSchema;
    Schema result = DataModelUtil.getReaderSchema(type, writerSchema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReaderSchemaForCompatibleSpecific() {
    Class<SmallEvent> type = SmallEvent.class;
    Schema writerSchema = StandardEvent.getClassSchema();
    Schema expResult = SmallEvent.getClassSchema();
    Schema result = DataModelUtil.getReaderSchema(type, writerSchema);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetReaderSchemaForCompatibleReflect() {
    Class<ReflectSmallEvent> type = ReflectSmallEvent.class;
    Schema writerSchema = ReflectData.get().getSchema(ReflectStandardEvent.class);
    Schema expResult = DataModelUtil.AllowNulls.get().getSchema(ReflectSmallEvent.class);
    Schema result = DataModelUtil.getReaderSchema(type, writerSchema);
    assertEquals(expResult, result);
  }

  @Test
  public void testCreateRecord() {
    assertNull("createRecord should not create Specific instances",
        DataModelUtil.createRecord(StandardEvent.class, StandardEvent.getClassSchema()));

    assertNull("createRecord should not create Reflect instances",
        DataModelUtil.createRecord(ReflectStandardEvent.class,
            ReflectData.get().getSchema(ReflectStandardEvent.class)));

    assertNotNull("createRecord should create Generic instances",
        DataModelUtil.createRecord(GenericData.Record.class,
            StandardEvent.getClassSchema()));

    assertEquals("createRecord did not return the expected class",
        TestGenericRecord.class,
        DataModelUtil.createRecord(TestGenericRecord.class,
            StandardEvent.getClassSchema()).getClass());
  }
}
