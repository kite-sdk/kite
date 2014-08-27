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
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetReaderException;
import org.kitesdk.data.UnknownFormatException;
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

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.*;
import org.kitesdk.data.impl.Accessor;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;

public class TestMultiFileDatasetReader extends TestDatasetReaders {

  public static final Path TEST_FILE = new Path(
      Resources.getResource("data/strings-100.avro").getFile());
  public static final Constraints CONSTRAINTS = new Constraints(STRING_SCHEMA);
  public static final RecordValidator<Record> VALIDATOR =
      new RecordValidator<Record>() {
      @Override
      public void validate(Record record, int recordNum) {
        Assert.assertNotNull(record);
        Assert.assertEquals(String.valueOf(recordNum % 100), record.get("text").toString());
      }
    };
  public static final DatasetDescriptor DESCRIPTOR = new DatasetDescriptor
      .Builder().schema(STRING_SCHEMA).build();
  private static final EntityAccessor<Record> ACCESSOR =
      DataModelUtil.accessor(Record.class, STRING_SCHEMA);

  @Override
  public DatasetReader newReader() throws IOException {
    return new MultiFileDatasetReader<Record>(
        FileSystem.get(new Configuration()),
        Lists.newArrayList(TEST_FILE, TEST_FILE),
        DESCRIPTOR, CONSTRAINTS, ACCESSOR);
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
        fileSystem, Lists.<Path>newArrayList(), DESCRIPTOR, CONSTRAINTS, ACCESSOR);

    checkReaderBehavior(reader, 0, VALIDATOR);
  }

  @Test
  public void testSingleFile() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE), DESCRIPTOR, CONSTRAINTS, ACCESSOR);

    checkReaderBehavior(reader, 100, VALIDATOR);
  }

  @Test(expected = NullPointerException.class)
  public void testRequriesFileSystem() throws IOException {
    new MultiFileDatasetReader<Record>(
        null, Lists.newArrayList(TEST_FILE, TEST_FILE), DESCRIPTOR,
        CONSTRAINTS, ACCESSOR);
  }

  @Test(expected = NullPointerException.class)
  public void testRequriesFiles() throws IOException {
    new MultiFileDatasetReader<Record>(
        fileSystem, null, DESCRIPTOR, CONSTRAINTS, ACCESSOR);
  }

  @Test(expected = NullPointerException.class)
  public void testRequriesDescriptor() throws IOException {
    new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE, TEST_FILE), null,
        CONSTRAINTS, ACCESSOR);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNullPaths() throws IOException {
    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(null, TEST_FILE), DESCRIPTOR,
        CONSTRAINTS, ACCESSOR);
    reader.initialize();
    reader.hasNext();
  }

  @Test(expected = UnknownFormatException.class)
  public void testUnknownFormat() throws IOException {
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(STRING_SCHEMA)
        .format(Accessor.getDefault().newFormat("explode!"))
        .build();

    MultiFileDatasetReader<Record> reader = new MultiFileDatasetReader<Record>(
        fileSystem, Lists.newArrayList(TEST_FILE), descriptor, CONSTRAINTS,
        ACCESSOR);

    try {
      reader.initialize();
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
        fileSystem, Lists.newArrayList(missingFile, TEST_FILE), DESCRIPTOR,
        CONSTRAINTS, ACCESSOR);

    try {
      try {
        reader.initialize();
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
          fileSystem, Lists.newArrayList(emptyFile, TEST_FILE), DESCRIPTOR,
          CONSTRAINTS, ACCESSOR);

      try {
        try {
          reader.initialize();
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
