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

package org.kitesdk.data.spi.filesystem;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;
import org.junit.Assert;
import org.junit.Test;

public class TestCSVSchemaInference {
  String csvLines = (
      "long,float,double,double2,string,nullable_long\n" +
      "34,12.3f,99.9d,81.0,s,\n" +
      "35,\"1.2f\",,,\"\",1234\n"
  );

  public Schema nullable(Schema.Type type) {
    return Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(type)));
  }

  public Schema schema(Schema.Type type) {
    return Schema.create(type);
  }

  @Test
  public void testSchemaInference() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().hasHeader().build());

    Assert.assertEquals("Should use name", "TestRecord", schema.getName());
    Assert.assertNull("Should not have namespace", schema.getNamespace());

    Assert.assertNotNull(schema.getField("long"));
    Assert.assertNotNull(schema.getField("float"));
    Assert.assertNotNull(schema.getField("double"));
    Assert.assertNotNull(schema.getField("double2"));
    Assert.assertNotNull(schema.getField("string"));
    Assert.assertNotNull(schema.getField("nullable_long"));

    Assert.assertEquals("Should infer a long",
        schema(Schema.Type.LONG), schema.getField("long").schema());
    Assert.assertEquals("Should infer a float (ends in f)",
        schema(Schema.Type.FLOAT), schema.getField("float").schema());
    Assert.assertEquals("Should infer a double (ends in d)",
        nullable(Schema.Type.DOUBLE), schema.getField("double").schema());
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("double2").schema());
    Assert.assertEquals("Should infer a string (not numeric)",
        nullable(Schema.Type.STRING), schema.getField("string").schema());
    Assert.assertEquals("Should infer a nullable long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("nullable_long").schema());
  }

  @Test
  public void testSchemaInferenceWithoutHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().build());

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));

    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_0").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_1").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_2").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_3").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_5").schema());
  }

  @Test
  public void testNullableSchemaInference() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferNullableSchema("TestRecord", stream,
        new CSVProperties.Builder().hasHeader().build());

    Assert.assertEquals("Should use name", "TestRecord", schema.getName());
    Assert.assertNull("Should not have namespace", schema.getNamespace());

    Assert.assertNotNull(schema.getField("long"));
    Assert.assertNotNull(schema.getField("float"));
    Assert.assertNotNull(schema.getField("double"));
    Assert.assertNotNull(schema.getField("double2"));
    Assert.assertNotNull(schema.getField("string"));
    Assert.assertNotNull(schema.getField("nullable_long"));

    Assert.assertEquals("Should infer a long",
        nullable(Schema.Type.LONG), schema.getField("long").schema());
    Assert.assertEquals("Should infer a float (ends in f)",
        nullable(Schema.Type.FLOAT), schema.getField("float").schema());
    Assert.assertEquals("Should infer a double (ends in d)",
        nullable(Schema.Type.DOUBLE), schema.getField("double").schema());
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("double2").schema());
    Assert.assertEquals("Should infer a string (not numeric)",
        nullable(Schema.Type.STRING), schema.getField("string").schema());
    Assert.assertEquals("Should infer a long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("nullable_long").schema());
  }

  @Test
  public void testNullableSchemaInferenceWithoutHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferNullableSchema("TestRecord", stream,
        new CSVProperties.Builder().build());

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));

    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_0").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_1").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_2").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_3").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_5").schema());
  }

  @Test
  public void testSchemaInferenceSkipHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().linesToSkip(1).build());

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));

    Assert.assertEquals("Should infer a long",
        schema(Schema.Type.LONG), schema.getField("field_0").schema());
    Assert.assertNull("Should not have a default value",
        schema.getField("field_0").defaultValue());
    Assert.assertEquals("Should infer a float (ends in f)",
        schema(Schema.Type.FLOAT), schema.getField("field_1").schema());
    Assert.assertNull("Should not have a default value",
        schema.getField("field_1").defaultValue());
    Assert.assertEquals("Should infer a double (ends in d)",
        nullable(Schema.Type.DOUBLE), schema.getField("field_2").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_2").defaultValue(), NullNode.getInstance());
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("field_3").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_3").defaultValue(), NullNode.getInstance());
    Assert.assertEquals("Should infer a string (not numeric)",
        nullable(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_4").defaultValue(), NullNode.getInstance());
    Assert.assertEquals("Should infer a long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("field_5").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_5").defaultValue(), NullNode.getInstance());
  }

  @Test
  public void testSchemaInferenceMissingExample() throws Exception {
    InputStream stream = new ByteArrayInputStream(
        "\none,two\n34,\n".getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().linesToSkip(1).hasHeader().build());

    Assert.assertNotNull(schema.getField("one"));
    Assert.assertNotNull(schema.getField("two"));

    Assert.assertEquals("Should infer a long",
        schema(Schema.Type.LONG), schema.getField("one").schema());
    Assert.assertEquals("Should default to a string",
        nullable(Schema.Type.STRING), schema.getField("two").schema());
  }

  @Test
  public void testSchemaNamespace() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferNullableSchema("com.example.TestRecord",
        stream, new CSVProperties.Builder().hasHeader().build());

    Assert.assertEquals("Should use name", "TestRecord", schema.getName());
    Assert.assertEquals("Should set namespace",
        "com.example", schema.getNamespace());
  }
}
