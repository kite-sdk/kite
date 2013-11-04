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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.TestDatasetReaders;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetReaderException;
import com.cloudera.cdk.data.UnknownFormatException;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.*;
import com.cloudera.cdk.data.impl.Accessor;
import org.apache.avro.generic.GenericData;

public class TestMultiFileDatasetReader extends TestDatasetReaders {

  public static final Path TEST_FILE = new Path(
      Resources.getResource("data/strings-100.avro").getFile());
  public static final RecordValidator<Record> VALIDATOR =
      new RecordValidator<Record>() {
      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertNotNull(record);
        Assert.assertEquals(String.valueOf(recordNum % 100), record.get("text"));
      }
    };
  public static final DatasetDescriptor DESCRIPTOR = new DatasetDescriptor
      .Builder().schema(STRING_SCHEMA).get();

  @Override
  public DatasetReader newReader() throws IOException {
    return new MultiFileDatasetReader<GenericData.Record>(
        FileSystem.get(new Configuration()),
        Lists.newArrayList(TEST_FILE, TEST_FILE),
        DESCRIPTOR);
  }

  @Override
  public int getTotalRecords() {
    return 200;
  }

  @Override
  public DatasetTestUtilities.RecordValidator getValidator() {
    return VALIDATOR;
  }

  private FileSystem fileSystem;
  @Before
  public void setUp() throws IOException {
    this.fileSystem = FileSystem.get(new Configuration());
  }

  @Test
  public void testEmptyPathList() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.<Path>newArrayList(), DESCRIPTOR);

    checkReaderBehavior(reader, 0, VALIDATOR);
  }

  @Test
  public void testSingleFile() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE), DESCRIPTOR);

    checkReaderBehavior(reader, 100, VALIDATOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequriesFileSystem() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        null, Lists.newArrayList(TEST_FILE, TEST_FILE), DESCRIPTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequriesFiles() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, null, DESCRIPTOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequriesDescriptor() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE, TEST_FILE), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNullPaths() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(null, TEST_FILE), DESCRIPTOR);
    reader.open();
    reader.hasNext();
  }

  @Test(expected = UnknownFormatException.class)
  public void testUnknownFormat() throws IOException {
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(STRING_SCHEMA)
        .format(Accessor.getDefault().newFormat("explode!"))
        .get();

    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE), descriptor);

    try {
      reader.open();
    } finally {
      reader.close();
    }
  }

  @Test(expected = DatasetReaderException.class)
  public void testMissingPath() throws IOException {
    Path missingFile = new Path("data/no-such-file.avro");

    /*
     * IMPORTANT: The DatasetReaderException should be thrown while iterating,
     * even though the first reader is the problem. This is because open()
     * should consistently validate the incoming files -- either fail when any
     * file is invalid or not check the validity of any files. Because we don't
     * want it to instantiate all FileSystemDatasetReaders in open(), this
     * verifies that the behavior is the latter case.
     */

    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(missingFile, TEST_FILE), DESCRIPTOR);

    try {
      try {
        reader.open();
      } catch (Throwable t) {
        Assert.fail("Reader failed in open: " + t.getClass().getName());
      }

      Assert.assertTrue("Reader is not open after open()", reader.isOpen());

      checkReaderIteration(reader, 200, VALIDATOR);

    } finally {
      reader.close();
    }
  }

  @Test(expected = DatasetReaderException.class)
  public void testEmptyFile() throws IOException {
    final Path emptyFile = new Path("/tmp/empty-file.avro");

    // outside the try block; if this fails then it isn't correct to remove it
    Assert.assertTrue("Failed to create a new empty file",
        fileSystem.createNewFile(emptyFile));

    /*
     * IMPORTANT: The DatasetReaderException should be thrown while iterating,
     * even though the first reader is the problem. This is because open()
     * should consistently validate the incoming files -- either fail when any
     * file is invalid or not check the validity of any files. Because we don't
     * want it to instantiate all FileSystemDatasetReaders in open(), this
     * verifies that the behavior is the latter case.
     */

    try {
      MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
          fileSystem, Lists.newArrayList(emptyFile, TEST_FILE), DESCRIPTOR);

      try {
        try {
          reader.open();
        } catch (Throwable t) {
          Assert.fail("Reader failed in open: " + t.getClass().getName());
        }

        Assert.assertTrue("Reader is not open after open()", reader.isOpen());

        // should fail in iteration
        checkReaderIteration(reader, 200, VALIDATOR);

      } finally {
        reader.close();
      }

    } finally {
      Assert.assertTrue("Failed to clean up empty file",
          fileSystem.delete(emptyFile, true));
    }
  }
}
