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
package com.cloudera.cdk.data.hcatalog;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cloudera.data.filesystem.DatasetTestUtilities.*;

public class TestHCatalogDatasetRepository {

  private static final Logger logger = LoggerFactory
      .getLogger(TestHCatalogDatasetRepository.class);

  private static final String TABLE_NAME = "test1";

  private FileSystem fileSystem;
  private Path testDirectory;
  private HCatalogDatasetRepository repo;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @Test
  public void testManagedTable() throws IOException {
    File tableDir = new File("/tmp/test/user/hive/warehouse/" + TABLE_NAME);
    Assert.assertFalse("Data directory should not exist before test", tableDir.exists());
    repo = new HCatalogDatasetRepository();

    Dataset ds = repo.create(TABLE_NAME,
        new DatasetDescriptor.Builder().schema(USER_SCHEMA_URL).get());
    Assert.assertTrue("Data directory should exist after dataset creation",
        tableDir.exists());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
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
        new DatasetDescriptor.Builder().schema(USER_SCHEMA_URL).get());
    Assert.assertTrue("Data directory should exist after dataset creation",
        tableDir.exists());

    writeTestUsers(ds, 10);
    checkTestUsers(ds, 10);
    Assert.assertTrue("Data directory should exist after writing", tableDir.exists());

    repo.drop(TABLE_NAME);
    Assert.assertFalse("Data directory should not exist after dropping",
        tableDir.exists());
  }

}
