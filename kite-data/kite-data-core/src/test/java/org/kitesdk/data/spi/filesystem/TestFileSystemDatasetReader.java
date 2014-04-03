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
package org.kitesdk.data.spi.filesystem;

import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetReaderException;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.*;
import org.apache.avro.generic.GenericData;

public class TestFileSystemDatasetReader extends TestDatasetReaders {

  @Override
  public DatasetReader newReader() throws IOException {
    return new FileSystemDatasetReader<String>(
        FileSystem.getLocal(new Configuration()),
        new Path(Resources.getResource("data/strings-100.avro").getFile()),
        STRING_SCHEMA);
  }

  @Override
  public int getTotalRecords() {
    return 100;
  }

  @Override
  public DatasetTestUtilities.RecordValidator getValidator() {
    return new DatasetTestUtilities.RecordValidator<GenericData.Record>() {
      @Override
      public void validate(GenericData.Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text"));
      }
    };
  }

  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
  }

  @Test
  public void testEvolvedSchema() throws IOException {
    Schema schema = Schema.createRecord("mystring", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Field("text", Schema.create(Type.STRING), null, null),
        new Field("text2", Schema.create(Type.STRING), null,
            JsonNodeFactory.instance.textNode("N/A"))));

    FileSystemDatasetReader<Record> reader = new FileSystemDatasetReader<Record>(
        fileSystem, new Path(Resources.getResource("data/strings-100.avro")
            .getFile()), schema);

    checkReaderBehavior(reader, 100, new RecordValidator<Record>() {
      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text"));
        Assert.assertEquals("N/A", record.get("text2"));
      }
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullFileSystem() {
    DatasetReader<String> reader = new FileSystemDatasetReader<String>(
        null, new Path("/tmp/does-not-exist.avro"), STRING_SCHEMA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullFile() {
    DatasetReader<String> reader = new FileSystemDatasetReader<String>(
        fileSystem, null, STRING_SCHEMA);
  }

  @Test(expected = DatasetReaderException.class)
  public void testMissingFile() {
    DatasetReader<String> reader = new FileSystemDatasetReader<String>(
        fileSystem, new Path("/tmp/does-not-exist.avro"), STRING_SCHEMA);

    // the reader should not fail until open()
    Assert.assertNotNull(reader);

    reader.open();
  }

  @Test(expected = DatasetReaderException.class)
  public void testEmptyFile() throws IOException {
    final Path emptyFile = new Path("/tmp/empty-file.avro");

    // outside the try block; if this fails then it isn't correct to remove it
    Assert.assertTrue("Failed to create a new empty file",
        fileSystem.createNewFile(emptyFile));

    try {
      DatasetReader<String> reader = new FileSystemDatasetReader<String>(
          fileSystem, emptyFile, STRING_SCHEMA);

      // the reader should not fail until open()
      Assert.assertNotNull(reader);

      reader.open();
    } finally {
      Assert.assertTrue("Failed to clean up empty file",
          fileSystem.delete(emptyFile, true));
    }
  }
}
