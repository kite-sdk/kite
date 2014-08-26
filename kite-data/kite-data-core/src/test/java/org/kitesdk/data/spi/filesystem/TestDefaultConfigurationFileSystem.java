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

import java.net.URI;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestDefaultConfigurationFileSystem extends MiniDFSTest {

  private static final DatasetRepository repo =
      new FileSystemDatasetRepository.Builder()
          .configuration(getConfiguration()) // explicitly use the DFS config
          .rootDirectory(URI.create("hdfs:/tmp/datasets"))
              .build();

  @Before
  public void createStringsDataset() throws Exception {
    // create a dataset in HDFS
    repo.delete("ns", "strings");
    repo.create("ns", "strings", new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build());
  }

  @After
  public void removeStringsDataset() {
    repo.delete("ns", "strings");
  }

  @Test
  public void testFindsHDFS() throws Exception {
    // set the default configuration that the loader will use
    Configuration existing = DefaultConfiguration.get();
    DefaultConfiguration.set(getConfiguration());

    FileSystemDataset<GenericRecord> dataset =
        Datasets.load("dataset:hdfs:/tmp/datasets/ns/strings");
    Assert.assertNotNull("Dataset should be found", dataset);
    Assert.assertEquals("Dataset should be located in HDFS",
        "hdfs", dataset.getFileSystem().getUri().getScheme());

    // replace the original config so the other tests are not affected
    DefaultConfiguration.set(existing);
  }

  @Test(expected=DatasetIOException.class)
  public void testCannotFindHDFS() throws Exception {
    // do not set the default configuration that the loader will use
    Datasets.load("dataset:hdfs:/tmp/datasets/ns/strings");
  }
}
