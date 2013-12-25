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
package org.kitesdk.morphline.hadoop.sequencefile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.kitesdk.morphline.api.AbstractMorphlineTest;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

public class ReadSequenceFileTest extends AbstractMorphlineTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadSequenceFileTest.class);

  /**
   * Test that Solr queries on a parsed SequenceFile document
   * return the expected content and fields.  Don't pass
   * in our own parser via the context.
   */
  @Test
  public void testSequenceFileContentSimple() throws Exception {
    morphline = createMorphline("test-morphlines/sequenceFileMorphlineSimple");
    String path = RESOURCES_DIR;
    File sequenceFile = new File(path, "testSequenceFileContentSimple.seq");
    int numRecords = 5;
    HashMap<String, Record>  expected = createTextSequenceFile(sequenceFile, numRecords);
    InputStream in = new FileInputStream(sequenceFile.getAbsolutePath());
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();

    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertTrue(areFieldsEqual(expected, collector.getRecords()));
  }

  /**
   * return a mapping of expected keys -> records
   */
  private HashMap<String, Record> createTextSequenceFile(File file, int numRecords) throws IOException {
    HashMap<String, Record> map = new HashMap<String, Record>();
    SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(), out, Text.class, Text.class,
        SequenceFile.CompressionType.NONE, null, metadata);
      for (int i = 0; i < numRecords; ++i) {
        Text key = new Text("key" + i);
        Text value = new Text("value" + i);
        writer.append(key, value);
        Record record = new Record();
        record.put("key", key);
        record.put("value", value);
        map.put(key.toString(), record);
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
    return map;
  }

  /**
   * Test that Solr queries on a parsed SequenceFile document
   * return the expected content and fields.
   */
  @Test
  public void testSequenceFileContentCustomParsers() throws Exception {
    morphline = createMorphline("test-morphlines/sequenceFileMorphlineSimple");
    String path = RESOURCES_DIR;
    File sequenceFile = new File(path, "testSequenceFileContentCustomParsers.seq");
    int numRecords = 10;
    HashMap<String, Record> expected = createTextSequenceFile(sequenceFile, numRecords);
    InputStream in = new FileInputStream(sequenceFile.getAbsolutePath());
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();

    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    
    assertTrue(areFieldsEqual(expected, collector.getRecords()));
  }

  /**
   * return a mapping of expected keys -> records
   */
  private HashMap<String, Record> createMyWritableSequenceFile(File file, int numRecords) throws IOException {
    HashMap<String, Record> map = new HashMap<String, Record>();
    SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(), out, Text.class, ParseTextMyWritableBuilder.MyWritable.class,
        SequenceFile.CompressionType.NONE, null, metadata);
      for (int i = 0; i < numRecords; ++i) {
        Text key = new Text("key" + i);
        ParseTextMyWritableBuilder.MyWritable value = new ParseTextMyWritableBuilder.MyWritable("value", i);
        writer.append(key, value);
        Record record = new Record();
        record.put("key", key);
        record.put("value", value);
        map.put(key.toString(), record);
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
    return map;
  }

  private TreeMap<Text, Text> getMetadataForSequenceFile() {
    TreeMap<Text, Text> metadata = new TreeMap<Text, Text>();
    metadata.put(new Text("license"), new Text("Apache"));
    metadata.put(new Text("year"), new Text("2013"));
    return metadata;

  }

  private boolean areRecordFieldsEqual(Record record1, Record record2, List<String> fieldsToCheck) {
    for(String field : fieldsToCheck) {
      if (!record1.get(field).equals(record2.get(field))) {
        return false;
      }
    }
    return true;
  }

  private boolean areFieldsEqual(HashMap<String, Record> expected, List<Record> actual) {
    if (expected.size() != actual.size()) {
      return false;
    }
    for (Record current : actual) {
      String key = current.getFirstValue("key").toString();
      Record currentExpected = expected.get(key);
      if (!areRecordFieldsEqual(current, currentExpected, Arrays.asList(new String[]{"key", "value"}))) {
        return false;
      }
    }

    return true;
  }
}
