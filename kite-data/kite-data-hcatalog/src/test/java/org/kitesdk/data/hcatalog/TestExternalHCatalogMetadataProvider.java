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

package org.kitesdk.data.hcatalog;

import org.kitesdk.data.DatasetDescriptor;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.kitesdk.data.hcatalog.impl.HCatalog;
import org.kitesdk.data.spi.MetadataProvider;
import org.kitesdk.data.spi.TestMetadataProviders;

public class TestExternalHCatalogMetadataProvider extends TestMetadataProviders {

  private Path testDirectory;

  public TestExternalHCatalogMetadataProvider(boolean distributed) {
    super(distributed);
  }

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    return new HCatalogExternalMetadataProvider(conf, testDirectory);
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    HCatalog hcat = new HCatalog(conf);
    for (String tableName : hcat.getAllTables("default")) {
      hcat.dropTable("default", tableName);
    }
  }

  @Test
  public void testCreateAssignsCorrectLocation() {
    ensureCreated();

    DatasetDescriptor loaded = provider.load(NAME);
    Path assignedPath = new Path(loaded.getLocation().getPath());
    Assert.assertEquals("Path should be in the test directory",
        new Path(testDirectory, NAME), assignedPath);
  }

  // TODO:
  // * check table metadata for Avro/Parquet and compatibility

}
