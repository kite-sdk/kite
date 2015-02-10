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

import org.apache.avro.SchemaBuilder;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.DatasetReader;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.*;
import org.kitesdk.data.spi.AbstractDatasetReader;

public class TestFileSystemDatasetReader extends TestDatasetReaders<Record> {

  @Override
  public DatasetReader<Record> newReader() throws IOException {
    return new FileSystemDatasetReader<Record>(
        LocalFileSystem.getInstance(),
        new Path(Resources.getResource("data/strings-100.avro").getFile()),
        STRING_SCHEMA, Record.class);
  }

  @Override
  public int getTotalRecords() {
    return 100;
  }

  @Override
  public DatasetTestUtilities.RecordValidator<Record> getValidator() {
    return new DatasetTestUtilities.RecordValidator<Record>() {
      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text").toString());
      }
    };
  }

  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    fileSystem = LocalFileSystem.getInstance();
  }

  @Test
  public void testEvolvedSchema() throws IOException {
    Schema schema = SchemaBuilder.record("mystring").fields()
        .requiredString("text")
        .name("text2").type().stringType().stringDefault("N/A")
        .endRecord();

    FileSystemDatasetReader<Record> reader = new FileSystemDatasetReader<Record>(
        fileSystem, new Path(Resources.getResource("data/strings-100.avro")
            .getFile()), schema, Record.class);

    checkReaderBehavior(reader, 100, new RecordValidator<Record>() {
      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertEquals(String.valueOf(recordNum), record.get("text").toString());
        Assert.assertEquals("N/A", record.get("text2").toString());
      }
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullFileSystem() {
    DatasetReader<String> reader = new FileSystemDatasetReader<String>(
        null, new Path("/tmp/does-not-exist.avro"), STRING_SCHEMA, String.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullFile() {
    DatasetReader<String> reader = new FileSystemDatasetReader<String>(
        fileSystem, null, STRING_SCHEMA, String.class);
  }

  @Test(expected = DatasetIOException.class)
  public void testMissingFile() {
    AbstractDatasetReader<String> reader = new FileSystemDatasetReader<String>(
        fileSystem, new Path("/tmp/does-not-exist.avro"), STRING_SCHEMA,
        String.class);

    // the reader should not fail until open()
    Assert.assertNotNull(reader);

    reader.initialize();
  }

  @Test(expected = DatasetIOException.class)
  public void testEmptyFile() throws IOException {
    final Path emptyFile = new Path("/tmp/empty-file.avro");

    // outside the try block; if this fails then it isn't correct to remove it
    Assert.assertTrue("Failed to create a new empty file",
        fileSystem.createNewFile(emptyFile));

    try {
      AbstractDatasetReader<String> reader = new FileSystemDatasetReader<String>(
          fileSystem, emptyFile, STRING_SCHEMA, String.class);

      // the reader should not fail until open()
      Assert.assertNotNull(reader);

      reader.initialize();
    } finally {
      Assert.assertTrue("Failed to clean up empty file",
          fileSystem.delete(emptyFile, true));
    }
  }
}
