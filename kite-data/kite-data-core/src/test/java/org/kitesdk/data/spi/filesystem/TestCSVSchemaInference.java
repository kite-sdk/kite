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

import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.TestHelpers;

public class TestCSVSchemaInference {
  String csvLines = (
      "long,float,double,double2,string,nullable_long,nullable_string\n" +
      "34,12.3f,99.9d,81.0,s,,\n" +
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
    Assert.assertNotNull(schema.getField("nullable_string"));

    Assert.assertEquals("Should infer a long",
        schema(Schema.Type.LONG), schema.getField("long").schema());
    Assert.assertEquals("Should infer a float (ends in f)",
        schema(Schema.Type.FLOAT), schema.getField("float").schema());
    Assert.assertEquals("Should infer a double (ends in d)",
        nullable(Schema.Type.DOUBLE), schema.getField("double").schema());
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("double2").schema());
    Assert.assertEquals("Should infer a non-null string (not numeric)",
        schema(Schema.Type.STRING), schema.getField("string").schema());
    Assert.assertEquals("Should infer a nullable long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("nullable_long").schema());
    Assert.assertEquals("Should infer a nullable string (second is missing)",
        nullable(Schema.Type.STRING),
        schema.getField("nullable_string").schema());
  }

  @Test
  public void testMissingRequiredFields() throws Exception {
    TestHelpers.assertThrows("Should fail: empty string found, required long",
        DatasetException.class, new Runnable() {
          @Override
          public void run() {
            try {
              CSVUtil.inferSchema("TestRecord",
                  new ByteArrayInputStream(csvLines.getBytes("utf8")),
                  new CSVProperties.Builder().hasHeader().build(),
                  ImmutableSet.of("nullable_long"));
            } catch (IOException e) {
              throw new RuntimeException("Schema inference threw IOException", e);
            }
          }
        });

    TestHelpers.assertThrows("Should fail: null found, required string",
        DatasetException.class, new Runnable() {
          @Override
          public void run() {
            try {
              CSVUtil.inferSchema("TestRecord",
                  new ByteArrayInputStream(csvLines.getBytes("utf8")),
                  new CSVProperties.Builder().hasHeader().build(),
                  ImmutableSet.of("nullable_string"));
            } catch (IOException e) {
              throw new RuntimeException("Schema inference threw IOException", e);
            }
          }
        });
  }

  @Test
  public void testSchemaInferenceWithoutHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().build(),
        ImmutableSet.of("float"));

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));
    Assert.assertNull(schema.getField("nullable_string"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));
    Assert.assertNotNull(schema.getField("field_6"));

    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_0").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_1").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_2").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_3").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertEquals("Header fields are all strings",
        schema(Schema.Type.STRING), schema.getField("field_5").schema());
    // field 6 is a nullable string because values are missing (short lines)
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_6").schema());
  }

  @Test
  public void testNullableSchemaInference() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferNullableSchema("TestRecord", stream,
        new CSVProperties.Builder().hasHeader().build(),
        ImmutableSet.of("float"));

    Assert.assertEquals("Should use name", "TestRecord", schema.getName());
    Assert.assertNull("Should not have namespace", schema.getNamespace());

    Assert.assertNotNull(schema.getField("long"));
    Assert.assertNotNull(schema.getField("float"));
    Assert.assertNotNull(schema.getField("double"));
    Assert.assertNotNull(schema.getField("double2"));
    Assert.assertNotNull(schema.getField("string"));
    Assert.assertNotNull(schema.getField("nullable_long"));
    Assert.assertNotNull(schema.getField("nullable_string"));

    Assert.assertEquals("Should infer a long",
        nullable(Schema.Type.LONG), schema.getField("long").schema());
    Assert.assertEquals("Should infer a non-null float (required, ends in f)",
        schema(Schema.Type.FLOAT), schema.getField("float").schema());
    Assert.assertEquals("Should infer a double (ends in d)",
        nullable(Schema.Type.DOUBLE), schema.getField("double").schema());
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("double2").schema());
    Assert.assertEquals("Should infer a string (not numeric)",
        nullable(Schema.Type.STRING), schema.getField("string").schema());
    Assert.assertEquals("Should infer a long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("nullable_long").schema());
    Assert.assertEquals("Should infer a nullable string (second is missing)",
        nullable(Schema.Type.STRING),
        schema.getField("nullable_string").schema());
  }

  @Test
  public void testNullableSchemaInferenceWithoutHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferNullableSchema("TestRecord", stream,
        new CSVProperties.Builder().build(),
        ImmutableSet.of("long", "field_1"));

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));
    Assert.assertNull(schema.getField("nullable_string"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));
    Assert.assertNotNull(schema.getField("field_6"));

    Assert.assertEquals("Header fields are all strings, not named long",
        nullable(Schema.Type.STRING), schema.getField("field_0").schema());
    Assert.assertEquals("Header fields are all strings, field_1 is required",
        schema(Schema.Type.STRING), schema.getField("field_1").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_2").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_3").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_5").schema());
    Assert.assertEquals("Header fields are all strings",
        nullable(Schema.Type.STRING), schema.getField("field_6").schema());
  }

  @Test
  public void testSchemaInferenceSkipHeader() throws Exception {
    InputStream stream = new ByteArrayInputStream(csvLines.getBytes("utf8"));
    Schema schema = CSVUtil.inferSchema("TestRecord", stream,
        new CSVProperties.Builder().linesToSkip(1).build(),
        ImmutableSet.of("long", "field_1"));

    Assert.assertNull(schema.getField("long"));
    Assert.assertNull(schema.getField("float"));
    Assert.assertNull(schema.getField("double"));
    Assert.assertNull(schema.getField("double2"));
    Assert.assertNull(schema.getField("string"));
    Assert.assertNull(schema.getField("nullable_long"));
    Assert.assertNull(schema.getField("nullable_string"));

    Assert.assertNotNull(schema.getField("field_0"));
    Assert.assertNotNull(schema.getField("field_1"));
    Assert.assertNotNull(schema.getField("field_2"));
    Assert.assertNotNull(schema.getField("field_3"));
    Assert.assertNotNull(schema.getField("field_4"));
    Assert.assertNotNull(schema.getField("field_5"));
    Assert.assertNotNull(schema.getField("field_6"));

    // avoid referring to Jackson API
    Object nullDefault = SchemaBuilder.record("Test").fields().optionalInt("f1")
        .endRecord().getFields().get(0).defaultValue();

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
        schema.getField("field_2").defaultValue(), nullDefault);
    Assert.assertEquals("Should infer a double (decimal defaults to double)",
        nullable(Schema.Type.DOUBLE), schema.getField("field_3").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_3").defaultValue(), nullDefault);
    Assert.assertEquals("Should infer a non-null string (not numeric)",
        schema(Schema.Type.STRING), schema.getField("field_4").schema());
    Assert.assertNull("Should not have a default value",
        schema.getField("field_4").defaultValue());
    Assert.assertEquals("Should infer a long (second line is a long)",
        nullable(Schema.Type.LONG), schema.getField("field_5").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_5").defaultValue(), nullDefault);
    Assert.assertEquals("Should infer a nullable string (second is missing)",
        nullable(Schema.Type.STRING), schema.getField("field_6").schema());
    Assert.assertEquals("Should have default value null",
        schema.getField("field_6").defaultValue(), nullDefault);
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

  @Test
  public void testSamplePrintableCharactersNotChanged() {
    String upper = "ABCDEFGHIJKLMNOPQRXTUVWXYZ";
    Assert.assertEquals("Upper case letters shouldn't be removed",
        upper, CSVUtil.sample(upper));
    String lower = "abcdefghijklmnopqrstuvwxyz";
    Assert.assertEquals("Lower case letters shouldn't be removed",
        lower, CSVUtil.sample(lower));
    String numbers = "0123456789";
    Assert.assertEquals("Numbers shouldn't be removed",
        numbers, CSVUtil.sample(numbers));
    String punctuation = " _-~+!@#$%^&*(){}[]<>,.?:;`'\"/\\|";
    Assert.assertEquals("Punctuation shouldn't be removed",
        punctuation, CSVUtil.sample(punctuation));
  }

  @Test
  public void testUnicodeRemoved() {
    String hasUnicode = "Unicode snowflake: \u2744";
    Assert.assertEquals("Should remove unicode",
        "Unicode snowflake: .", CSVUtil.sample(hasUnicode));
  }

  @Test
  public void testSampleTruncated() {
    String longUrl = "https://github.com/kite-sdk/kite/commit/" +
        "bbe3e917875e879ca58b8afe90efa96cdd4691d1";
    Assert.assertEquals("Should truncate long values",
        "https://github.com/kite-sdk/kite/commit/bbe3e91787",
        CSVUtil.sample(longUrl));
  }

  @Test
  public void testSampleNull() {
    String nullString = null;
    Assert.assertEquals("Should handle null like String.valueOf",
        String.valueOf(nullString), CSVUtil.sample(nullString));
  }
}
