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

package org.kitesdk.data.hcatalog;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

public class TestSchemaConversion {

  private static final Schema SIMPLE_RECORD =
      SchemaBuilder.record("SimpleRecord").fields()
          .name("id").type().intType().noDefault()
          .name("name").type().stringType().noDefault()
          .endRecord();
  private static final Schema COMPLEX_RECORD =
      SchemaBuilder.record("ComplexRecord").fields()
          .name("groupName").type().stringType().noDefault()
          .name("simpleRecords").type().array().items()
              .type(SIMPLE_RECORD).noDefault()
          .endRecord();

  private static final Function<FieldSchema, String> GET_NAMES =
      new Function<FieldSchema, String>() {
        @Override
        public String apply(@Nullable FieldSchema input) {
          if (input != null) {
            return input.getName();
          } else {
            return null;
          }
        }
      };
  private static final Function<FieldSchema, String> GET_TYPE_STRINGS =
      new Function<FieldSchema, String>() {
        @Override
        public String apply(@Nullable FieldSchema input) {
          if (input != null) {
            return input.getType();
          } else {
            return null;
          }
        }
      };

  @Test
  public void testConvertSchemaWithPrimitive() {
    Schema primitiveSchema = SchemaBuilder.builder().stringType();
    List<FieldSchema> fields = HiveUtils.convertSchema(primitiveSchema);

    Assert.assertEquals("Should be a single FieldSchema", 1, fields.size());
    Assert.assertEquals("Should be named \"column\"",
        "column", fields.get(0).getName());
    Assert.assertEquals("Should be named \"column\"",
        TypeInfoFactory.stringTypeInfo.toString(), fields.get(0).getType());
  }

  @Test
  public void testConvertSchemaWithSimpleRecord() {
    // convertSchema returns a list of FieldSchema objects rather than TypeInfo
    List<FieldSchema> fields = HiveUtils.convertSchema(SIMPLE_RECORD);

    Assert.assertEquals("Field names should match",
        Lists.newArrayList("id", "name"),
        Lists.transform(fields, GET_NAMES));
    Assert.assertEquals("Field types should match",
        Lists.newArrayList(
            TypeInfoFactory.intTypeInfo.toString(),
            TypeInfoFactory.stringTypeInfo.toString()),
        Lists.transform(fields, GET_TYPE_STRINGS));
  }

  @Test
  public void testConvertSchemaWithComplexRecord() {
    // convertSchema returns a list of FieldSchema objects rather than TypeInfo
    List<FieldSchema> fields = HiveUtils.convertSchema(COMPLEX_RECORD);

    Assert.assertEquals("Field names should match",
        Lists.newArrayList("groupName", "simpleRecords"),
        Lists.transform(fields, GET_NAMES));
    Assert.assertEquals("Field types should match",
        Lists.newArrayList(
            TypeInfoFactory.stringTypeInfo.toString(),
            TypeInfoFactory.getListTypeInfo(
                TypeInfoFactory.getStructTypeInfo(
                    Lists.newArrayList("id", "name"),
                    Lists.newArrayList(
                        TypeInfoFactory.intTypeInfo,
                        TypeInfoFactory.stringTypeInfo))).toString()),
        Lists.transform(fields, GET_TYPE_STRINGS));
  }

  @Test
  public void testSimpleRecord() {
    TypeInfo type = HiveUtils.convert(SIMPLE_RECORD);

    Assert.assertTrue("Record should be converted to struct",
        type instanceof StructTypeInfo);
    Assert.assertEquals("Field names should match",
        Lists.newArrayList("id", "name"),
        ((StructTypeInfo) type).getAllStructFieldNames());
    Assert.assertEquals("Field types should match",
        Lists.newArrayList(
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.stringTypeInfo),
        ((StructTypeInfo) type).getAllStructFieldTypeInfos());
  }

  @Test
  public void testArray() {
    TypeInfo type = HiveUtils.convert(SchemaBuilder.array()
        .items().floatType());

    Assert.assertEquals("Array should be converted to list",
        TypeInfoFactory.getListTypeInfo(TypeInfoFactory.floatTypeInfo),
        type);
  }

  @Test
  public void testMap() {
    TypeInfo type = HiveUtils.convert(SchemaBuilder.builder().map()
        .values().booleanType());

    Assert.assertEquals("Map should be converted to map",
        TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.booleanTypeInfo),
        type);
  }

  @Test
  public void testUnion() {
    TypeInfo type = HiveUtils.convert(SchemaBuilder.builder().unionOf()
        .bytesType().and()
        .fixed("fixed").size(12).and()
        .doubleType().and()
        .longType()
        .endUnion());

    Assert.assertEquals("Union should be converted to union",
        TypeInfoFactory.getUnionTypeInfo(Lists.newArrayList(
            TypeInfoFactory.binaryTypeInfo,
            TypeInfoFactory.binaryTypeInfo,
            TypeInfoFactory.doubleTypeInfo,
            TypeInfoFactory.longTypeInfo)),
        type);
  }

  @Test
  public void testEnum() {
    TypeInfo type = HiveUtils.convert(SchemaBuilder.builder()
        .enumeration("TestEnum").symbols("a", "b", "c"));

    Assert.assertEquals("Enum should be converted to string",
        TypeInfoFactory.stringTypeInfo, type);
  }

  @Test(expected=IllegalStateException.class)
  public void testRecursiveRecord() {
    Schema recursiveRecord = SchemaBuilder.record("RecursiveRecord").fields()
        .name("name").type().stringType().noDefault()
        .name("children").type().array().items()
            .type("RecursiveRecord").noDefault()
        .endRecord();
    HiveUtils.convert(recursiveRecord);
  }
}
