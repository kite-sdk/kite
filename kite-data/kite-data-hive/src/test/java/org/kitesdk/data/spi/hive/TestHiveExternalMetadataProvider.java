/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.hive;

import org.junit.Before;
import org.kitesdk.data.DatasetDescriptor;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.MetadataProvider;
import org.kitesdk.data.spi.TestMetadataProviders;

public class TestHiveExternalMetadataProvider extends TestMetadataProviders {

  private Path testDirectory;

  public TestHiveExternalMetadataProvider(boolean distributed) {
    super(distributed);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    return new HiveExternalMetadataProvider(conf, testDirectory);
  }

  @Before
  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(getConfiguration());
    for (String database : metastore.getAllDatabases()) {
      for (String table : metastore.getAllTables(database)) {
        metastore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        metastore.dropDatabase(database, true);
      }
    }
  }

  @Test
  public void testCreateAssignsCorrectLocation() {
    ensureCreated();

    DatasetDescriptor loaded = provider.load(NAMESPACE, NAME);
    Path assignedPath = new Path(loaded.getLocation().getPath());
    Assert.assertEquals("Path should be in the test directory",
        new Path(testDirectory, new Path(NAMESPACE, NAME)), assignedPath);
  }

  @Test
  public void testRejectsDuplicatePartitionNames() {
    final DatasetDescriptor descriptor = new DatasetDescriptor.Builder(testDescriptor)
        .partitionStrategy(new PartitionStrategy.Builder()
            .identity("timestamp", "timestamp")
            .build())
        .build();
    TestHelpers.assertThrows("Should reject duplicate field and partition name",
        ValidationException.class, new Runnable() {
      @Override
      public void run() {
        provider.create(NAMESPACE, "reject", descriptor);
      }
    });
  }

  // TODO:
  // * check table metadata for Avro/Parquet and compatibility

}
