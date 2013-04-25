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
package com.cloudera.data.hcatalog;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.DatasetReader;
import com.cloudera.data.DatasetWriter;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHCatalogDatasetRepository {

  private static final Logger logger = LoggerFactory
      .getLogger(TestHCatalogDatasetRepository.class);

  private static final String TABLE_NAME = "test1";

  private FileSystem fileSystem;
  private Path testDirectory;
  private Schema testSchema;
  private HCatalogDatasetRepository repo;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    testSchema = new Schema.Parser().parse(Resources.getResource(
        "schema/user.avsc").openStream());
  }

  @Test
  public void testManagedTable() throws IOException {
    File tableDir = new File("/tmp/test/user/hive/warehouse/" + TABLE_NAME);
    Assert.assertFalse("Data directory should not exist before test", tableDir.exists());
    repo = new HCatalogDatasetRepository();

    Dataset ds = repo.create(TABLE_NAME,
        new DatasetDescriptor.Builder().schema(testSchema).get());
    Assert.assertTrue("Data directory should exist after dataset creation",
        tableDir.exists());

    testWriteAndRead(ds);
    Assert.assertTrue("Data directory should exist after writing", tableDir.exists());

    repo.drop(TABLE_NAME);
    Assert.assertFalse("Data directory should not exist after dropping",
        tableDir.exists());
  }

  @Test
  public void testExternalTable() throws IOException {
    File tableDir = new File(testDirectory.toString(), TABLE_NAME);
    Assert.assertFalse("Data directory should not exist before test", tableDir.exists());
    repo = new HCatalogDatasetRepository(fileSystem, testDirectory);

    Dataset ds = repo.create(TABLE_NAME,
        new DatasetDescriptor.Builder().schema(testSchema).get());
    Assert.assertTrue("Data directory should exist after dataset creation",
        tableDir.exists());

    testWriteAndRead(ds);
    Assert.assertTrue("Data directory should exist after writing", tableDir.exists());

    repo.drop(TABLE_NAME);
    Assert.assertFalse("Data directory should not exist after dropping",
        tableDir.exists());
  }

  private void testWriteAndRead(Dataset ds) throws IOException {


    logger.debug("Writing to dataset:{}", ds);

    Assert.assertFalse("Dataset is not partition", ds.getDescriptor()
        .isPartitioned());

    /*
     * Turns out ReflectDatumWriter subclasses GenericDatumWriter so this
     * actually works.
     */
    GenericData.Record record = new GenericRecordBuilder(testSchema)
        .set("username", "test").set("email", "email-test").build();

    DatasetWriter<GenericData.Record> writer = null;

    try {
      // TODO: Fix the cast situation. (Leave this warning until we do.)
      writer = ds.getWriter();

      writer.open();

      Assert.assertNotNull("Get writer produced a writer", writer);

      writer.write(record);
      writer.flush();
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    DatasetReader<GenericData.Record> reader = null;
    try {
      reader = ds.getReader();
      reader.open();
      Assert.assertTrue(reader.hasNext());
      GenericData.Record actualRecord = reader.read();
      Assert.assertEquals(record.get("username"), actualRecord.get("username"));
      Assert.assertEquals(record.get("email"), actualRecord.get("email"));
      Assert.assertFalse(reader.hasNext());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

  }
}
