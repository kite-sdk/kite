/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.hive;

import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;

import static org.kitesdk.data.spi.hive.HiveSchemaConverter.NO_REQUIRED_FIELDS;
import static org.kitesdk.data.spi.hive.HiveSchemaConverter.NULL_DEFAULT;
import static org.kitesdk.data.spi.hive.HiveSchemaConverter.optional;
import static org.kitesdk.data.spi.hive.HiveSchemaConverter.parseTypeInfo;

public class TestTableConversion {

  private final LinkedList<String> startPath = Lists.newLinkedList();

  private Schema convertPrimitive(String type) {
    return HiveSchemaConverter.convert(startPath, "test",
        parseTypeInfo(type), NO_REQUIRED_FIELDS);
  }

  @Test
  public void testConvertPrimitives() {
    Assert.assertEquals(
        Schema.create(Schema.Type.BOOLEAN), convertPrimitive("boolean"));
    Assert.assertEquals(
        Schema.create(Schema.Type.INT), convertPrimitive("tinyint"));
    Assert.assertEquals(
        Schema.create(Schema.Type.INT), convertPrimitive("smallint"));
    Assert.assertEquals(
        Schema.create(Schema.Type.INT), convertPrimitive("int"));
    Assert.assertEquals(
        Schema.create(Schema.Type.LONG), convertPrimitive("bigint"));
    Assert.assertEquals(
        Schema.create(Schema.Type.FLOAT), convertPrimitive("float"));
    Assert.assertEquals(
        Schema.create(Schema.Type.DOUBLE), convertPrimitive("double"));
    Assert.assertEquals(
        Schema.create(Schema.Type.STRING), convertPrimitive("string"));
    Assert.assertEquals(
        Schema.create(Schema.Type.BYTES), convertPrimitive("binary"));

    if (HiveSchemaConverter.charClass != null) {
      Assert.assertEquals(
          Schema.create(Schema.Type.STRING), convertPrimitive("char(10)"));
    }

    if (HiveSchemaConverter.varcharClass != null) {
      Assert.assertEquals(
          Schema.create(Schema.Type.STRING), convertPrimitive("varchar(32)"));
    }

    if (HiveSchemaConverter.decimalClass != null) {
      TestHelpers.assertThrows("Should reject unknown type",
          IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
              convertPrimitive("decimal(2,4)");
            }
          });
    }
  }

  @Test
  public void testConvertArrays() {
    TypeInfo arrayOfStringsType = parseTypeInfo("array<string>");
    Schema arrayOfStringsSchema = Schema.createArray(
        optional(Schema.create(Schema.Type.STRING)));
    Assert.assertEquals("Should convert array of primitive",
        arrayOfStringsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", arrayOfStringsType, NO_REQUIRED_FIELDS));

    TypeInfo arrayOfArraysType = parseTypeInfo("array<array<string>>");
    Schema arrayOfArraysSchema = Schema.createArray(
        optional(arrayOfStringsSchema));
    Assert.assertEquals("Should convert array of arrays",
        arrayOfArraysSchema,
        HiveSchemaConverter.convert(
            startPath, "test", arrayOfArraysType, NO_REQUIRED_FIELDS));

    TypeInfo arrayOfMapsType = parseTypeInfo("array<map<string,float>>");
    Schema arrayOfMapsSchema = Schema.createArray(
        optional(Schema.createMap(optional(Schema.create(Schema.Type.FLOAT)))));
    Assert.assertEquals("Should convert array of maps",
        arrayOfMapsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", arrayOfMapsType, NO_REQUIRED_FIELDS));

    TypeInfo arrayOfStructsType = parseTypeInfo(
        "array<struct<a:array<array<string>>,b:array<map<string,float>>>>");
    Schema recordSchema = Schema.createRecord("test", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a", optional(arrayOfArraysSchema), null, NULL_DEFAULT),
        new Schema.Field("b", optional(arrayOfMapsSchema), null, NULL_DEFAULT)
    ));
    Schema arrayOfStructsSchema = Schema.createArray(optional(recordSchema));
    Assert.assertEquals("Should convert array of structs",
        arrayOfStructsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", arrayOfStructsType, NO_REQUIRED_FIELDS));
  }

  @Test
  public void testConvertMaps() {
    TypeInfo mapOfLongsType = parseTypeInfo("map<string,bigint>");
    Schema mapOfLongsSchema = Schema.createMap(
        optional(Schema.create(Schema.Type.LONG)));
    Assert.assertEquals("Should convert map of primitive",
        mapOfLongsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", mapOfLongsType, NO_REQUIRED_FIELDS));

    TypeInfo mapOfArraysType = parseTypeInfo("array<float>");
    Schema mapOfArraysSchema = Schema.createArray(
        optional(Schema.create(Schema.Type.FLOAT)));
    Assert.assertEquals("Should convert map of arrays",
        mapOfArraysSchema,
        HiveSchemaConverter.convert(
            startPath, "test", mapOfArraysType, NO_REQUIRED_FIELDS));

    TypeInfo mapOfMapsType = parseTypeInfo(
        "array<map<string,map<string,bigint>>>");
    Schema mapOfMapsSchema = Schema.createArray(
        optional(Schema.createMap(optional(mapOfLongsSchema))));
    Assert.assertEquals("Should convert map of maps",
        mapOfMapsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", mapOfMapsType, NO_REQUIRED_FIELDS));

    TypeInfo mapOfStructsType = parseTypeInfo("map<string," +
        "struct<a:array<float>,b:array<map<string,map<string,bigint>>>>>");
    Schema recordSchema = Schema.createRecord("test", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a", optional(mapOfArraysSchema), null, NULL_DEFAULT),
        new Schema.Field("b", optional(mapOfMapsSchema), null, NULL_DEFAULT)
    ));
    Schema mapOfStructsSchema = Schema.createMap(optional(recordSchema));
    Assert.assertEquals("Should convert map of structs",
        mapOfStructsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", mapOfStructsType, NO_REQUIRED_FIELDS));
  }

  private static final TypeInfo STRUCT_OF_STRUCTS_TYPE = parseTypeInfo(
      "struct<str:string,inner:struct<a:int,b:binary>>");

  @Test
  public void testConvertStructs() {
    Schema recordSchema = Schema.createRecord("inner", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a",
            optional(Schema.create(Schema.Type.INT)), null, NULL_DEFAULT),
        new Schema.Field("b",
            optional(Schema.create(Schema.Type.BYTES)), null, NULL_DEFAULT)
    ));
    Schema structOfStructsSchema = Schema.createRecord("test", null, null, false);
    structOfStructsSchema.setFields(Lists.newArrayList(
        new Schema.Field("str",
            optional(Schema.create(Schema.Type.STRING)), null, NULL_DEFAULT),
        new Schema.Field("inner", optional(recordSchema), null, NULL_DEFAULT)
    ));

    Assert.assertEquals("Should convert struct of structs",
        structOfStructsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", STRUCT_OF_STRUCTS_TYPE, NO_REQUIRED_FIELDS));
  }

  @Test
  public void testConvertStructWithRequiredFields() {
    Schema recordSchema = Schema.createRecord("inner", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("b",
            optional(Schema.create(Schema.Type.BYTES)), null, NULL_DEFAULT)
    ));
    Schema structOfStructsSchema = Schema.createRecord("test", null, null, false);
    structOfStructsSchema.setFields(Lists.newArrayList(
        new Schema.Field("str", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("inner", recordSchema, null, null)
    ));

    Assert.assertEquals("Should convert struct of structs",
        structOfStructsSchema,
        HiveSchemaConverter.convert(
            startPath, "test", STRUCT_OF_STRUCTS_TYPE,
            Lists.newArrayList(
                new String[]{"test", "str"},
                new String[]{"test", "inner", "a"})));
  }

  private static final List<FieldSchema> TABLE = Lists.newArrayList(
      new FieldSchema("str", "string", null),
      new FieldSchema("inner", "struct<a:int,b:binary>", null)
  );

  @Test
  public void testConvertTable() {
    Schema recordSchema = Schema.createRecord("inner", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a",
            optional(Schema.create(Schema.Type.INT)), null, NULL_DEFAULT),
        new Schema.Field("b",
            optional(Schema.create(Schema.Type.BYTES)), null, NULL_DEFAULT)
    ));
    Schema structOfStructsSchema = Schema.createRecord("test", null, null, false);
    structOfStructsSchema.setFields(Lists.newArrayList(
        new Schema.Field("str",
            optional(Schema.create(Schema.Type.STRING)), null, NULL_DEFAULT),
        new Schema.Field("inner", optional(recordSchema), null, NULL_DEFAULT)
    ));

    Assert.assertEquals("Should convert struct of structs",
        structOfStructsSchema,
        HiveSchemaConverter.convertTable("test", TABLE, null));
  }

  @Test
  public void testConvertTableWithRequiredFields() {
    Schema recordSchema = Schema.createRecord("inner", null, null, false);
    recordSchema.setFields(Lists.newArrayList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("b",
            optional(Schema.create(Schema.Type.BYTES)), null, NULL_DEFAULT)
    ));
    Schema structOfStructsSchema = Schema.createRecord("test", null, null, false);
    structOfStructsSchema.setFields(Lists.newArrayList(
        new Schema.Field("str", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("inner", recordSchema, null, null)
    ));

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("not_present", "int")
        .hash("inner.a", 16) // requires both inner and inner.a
        .identity("str")
        .build();

    Assert.assertEquals("Should convert table named test",
        structOfStructsSchema,
        HiveSchemaConverter.convertTable("test", TABLE, strategy));
  }
}
