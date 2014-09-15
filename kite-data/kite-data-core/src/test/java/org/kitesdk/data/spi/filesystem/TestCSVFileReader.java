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

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.TestHelpers;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import org.kitesdk.data.spi.DataModelUtil;

public class TestCSVFileReader extends TestDatasetReaders<GenericData.Record> {
  /*
   * OpenCSV notes:
   * - An empty unquoted field is passed as an empty string
   */

  public static final String CSV_CONTENT = (
      "str,34,2.11,false\r\n" +
      "\"str,2\",,4,true\n" +
      "str3,\"\",null");

  public static final String VALIDATOR_CSV_CONTENT =
      "id,string,even\n" +
          "0,a,true\n" +
          "1,b\n" +
          "2,c,true\n";

  public static final String TSV_CONTENT = (
      "string\tinteger\tfloat\tbool\r" +
      "str\t34\t2.11\tfalse\r\n" +
      "\"str\t2\"\t\t4\ttrue\n" +
      "str3\t\"\"\tnull");

  public static FileSystem localfs = null;
  public static Path csvFile = null;
  public static Path validatorFile = null;
  public static Path tsvFile = null;

  public static Schema STRINGS = SchemaBuilder.record("Strings")
      .fields()
      .name("string1").type().stringType().noDefault()
      .name("string2").type().stringType().noDefault()
      .name("string3").type().stringType().noDefault()
      .name("string4").type().stringType().stringDefault("missing value")
      .endRecord();

  public static final Schema VALIDATOR_SCHEMA = SchemaBuilder.record("Validator")
      .fields()
      .name("id").type().intType().noDefault()
      .name("string").type().stringType().noDefault()
      .name("even").type().booleanType().booleanDefault(false)
      .endRecord();

  public static Schema BEAN_SCHEMA = SchemaBuilder.record(TestBean.class.getName())
      .fields()
      .name("myStr").type().stringType().noDefault()
      .name("myInt").type().intType().intDefault(0)
      .name("myFloat").type().floatType().noDefault()
      .name("myBool").type().booleanType().booleanDefault(false)
      .endRecord();

  public static Schema SCHEMA = SchemaBuilder.record("Normal")
      .fields()
      .name("myString").type().stringType().noDefault()
      .name("myInt").type().intType().intDefault(0)
      .name("myFloat").type().floatType().noDefault()
      .name("myBool").type().booleanType().booleanDefault(false)
      .endRecord();

  @BeforeClass
  public static void createCSVFiles() throws IOException {
    localfs = FileSystem.getLocal(new Configuration());
    csvFile = new Path("target/temp.csv");
    tsvFile = new Path("target/temp.tsv");
    validatorFile = new Path("target/validator.csv");

    FSDataOutputStream out = localfs.create(csvFile, true);
    out.writeBytes(CSV_CONTENT);
    out.close();

    out = localfs.create(validatorFile, true);
    out.writeBytes(VALIDATOR_CSV_CONTENT);
    out.close();

    out = localfs.create(tsvFile, true);
    out.writeBytes(TSV_CONTENT);
    out.close();
  }

  @Override
  public DatasetReader<GenericData.Record> newReader() throws IOException {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .property("kite.csv.has-header", "true")
        .schema(VALIDATOR_SCHEMA)
        .build();
    return new CSVFileReader<GenericData.Record>(localfs, validatorFile, desc,
        DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));
  }

  @Override
  public int getTotalRecords() {
    return 3;
  }

  @Override
  public DatasetTestUtilities.RecordValidator<GenericData.Record> getValidator() {
    return new DatasetTestUtilities.RecordValidator<GenericData.Record>() {
      private static final String chars = "abcdef";
      @Override
      public void validate(GenericData.Record record, int recordNum) {
        Assert.assertEquals(recordNum, record.get("id"));
        Assert.assertEquals(Character.toString(chars.charAt(recordNum)), record.get("string"));
        Assert.assertEquals((recordNum % 2) == 0, record.get("even"));
      }
    };
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNonRecordSchemas() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(SchemaBuilder.array().items().stringType())
        .build();
    new CSVFileReader<GenericData.Record>(localfs, csvFile, desc,
        DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));
  }

  @Test
  public void testStringSchema() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(STRINGS)
        .build();
    final CSVFileReader<GenericData.Record> reader =
        new CSVFileReader<GenericData.Record>(localfs, csvFile, desc,
            DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    GenericData.Record rec = reader.next();
    Assert.assertEquals("str", rec.get(0));
    Assert.assertEquals("34", rec.get(1));
    Assert.assertEquals("2.11", rec.get(2));
    Assert.assertEquals("false", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str,2", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("4", rec.get(2));
    Assert.assertEquals("true", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str3", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("null", rec.get(2));
    Assert.assertEquals("missing value", rec.get(3));

    Assert.assertFalse(reader.hasNext());
  }

  @Test
  public void testTSV() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .property("kite.csv.delimiter", "\t")
        .property("kite.csv.lines-to-skip", "1")
        .schema(STRINGS)
        .build();
    final CSVFileReader<GenericData.Record> reader =
        new CSVFileReader<GenericData.Record>(localfs, tsvFile, desc,
            DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    GenericData.Record rec = reader.next();
    Assert.assertEquals("str", rec.get(0));
    Assert.assertEquals("34", rec.get(1));
    Assert.assertEquals("2.11", rec.get(2));
    Assert.assertEquals("false", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str\t2", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("4", rec.get(2));
    Assert.assertEquals("true", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str3", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("null", rec.get(2));
    Assert.assertEquals("missing value", rec.get(3));

    Assert.assertFalse(reader.hasNext());
  }

  @Test
  public void testTSVWithDeprecatedProperties() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .property("cdk.csv.delimiter", "\t")
        .property("cdk.csv.lines-to-skip", "1")
        .schema(STRINGS)
        .build();
    final CSVFileReader<GenericData.Record> reader =
        new CSVFileReader<GenericData.Record>(localfs, tsvFile, desc,
            DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    GenericData.Record rec = reader.next();
    Assert.assertEquals("str", rec.get(0));
    Assert.assertEquals("34", rec.get(1));
    Assert.assertEquals("2.11", rec.get(2));
    Assert.assertEquals("false", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str\t2", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("4", rec.get(2));
    Assert.assertEquals("true", rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str3", rec.get(0));
    Assert.assertEquals("", rec.get(1));
    Assert.assertEquals("null", rec.get(2));
    Assert.assertEquals("missing value", rec.get(3));

    Assert.assertFalse(reader.hasNext());
  }

  @Test
  public void testNormalSchema() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(SCHEMA)
        .build();
    final CSVFileReader<GenericData.Record> reader =
        new CSVFileReader<GenericData.Record>(localfs, csvFile, desc,
            DataModelUtil.accessor(GenericData.Record.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    GenericData.Record rec = reader.next();
    Assert.assertEquals("str", rec.get(0));
    Assert.assertEquals(34, rec.get(1));
    Assert.assertEquals(2.11f, rec.get(2));
    Assert.assertEquals(false, rec.get(3));

    Assert.assertTrue(reader.hasNext());
    rec = reader.next();
    Assert.assertEquals("str,2", rec.get(0));
    Assert.assertEquals(0, rec.get(1));
    Assert.assertEquals(4.0f, rec.get(2));
    Assert.assertEquals(true, rec.get(3));

    Assert.assertTrue(reader.hasNext());
    TestHelpers.assertThrows("Should complain about missing default",
        AvroRuntimeException.class, new Runnable() {
      @Override
      public void run() {
        reader.next();
      }
    });

    Assert.assertFalse(reader.hasNext());
  }

  @Test
  public void testReflectedRecords() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(BEAN_SCHEMA)
        .build();
    final CSVFileReader<TestBean> reader =
        new CSVFileReader<TestBean>(localfs, csvFile, desc,
            DataModelUtil.accessor(TestBean.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    TestBean bean = reader.next();
    Assert.assertEquals("str", bean.myStr);
    Assert.assertEquals((Integer) 34, bean.myInt);
    Assert.assertEquals((Float) 2.11f, bean.myFloat);
    Assert.assertEquals(false, bean.myBool);

    Assert.assertTrue(reader.hasNext());
    bean = reader.next();
    Assert.assertEquals("str,2", bean.myStr);
    Assert.assertEquals(null, bean.myInt);
    Assert.assertEquals((Float) 4.0f, bean.myFloat);
    Assert.assertEquals(true, bean.myBool);

    Assert.assertTrue(reader.hasNext());
    bean = reader.next();
    Assert.assertEquals("str3", bean.myStr);
    Assert.assertEquals(null, bean.myInt);
    Assert.assertEquals(null, bean.myFloat);
    Assert.assertEquals(null, bean.myBool);

    Assert.assertFalse(reader.hasNext());
  }

  @Test
  public void testCustomGenericRecords() {
    final DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(SCHEMA)
        .build();
    final CSVFileReader<TestGenericRecord> reader =
        new CSVFileReader<TestGenericRecord>(localfs, csvFile, desc,
        DataModelUtil.accessor(TestGenericRecord.class, desc.getSchema()));

    reader.initialize();
    Assert.assertTrue(reader.hasNext());
    TestGenericRecord record = reader.next();
    Assert.assertEquals("str", record.get(0));
    Assert.assertEquals((Integer) 34, record.get(1));
    Assert.assertEquals((Float) 2.11f, record.get(2));
    Assert.assertEquals(false, record.get(3));

    Assert.assertTrue(reader.hasNext());
    record = reader.next();
    Assert.assertEquals("str,2", record.get(0));
    Assert.assertEquals((Integer) 0, record.get(1));
    Assert.assertEquals((Float) 4.0f, record.get(2));
    Assert.assertEquals(true, record.get(3));

    Assert.assertTrue(reader.hasNext());
    TestHelpers.assertThrows("Should complain about missing default",
        AvroRuntimeException.class, new Runnable() {
      @Override
      public void run() {
        reader.next();
      }
    });

    Assert.assertFalse(reader.hasNext());
  }
}
