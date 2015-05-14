/*
 * Copyright 2015 Cloudera, Inc.
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
package org.kitesdk.data.spi.filesystem;

import com.google.common.io.Files;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.PartitionListener;
import org.kitesdk.data.spi.StorageKey;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;

public class TestDatasetWriterCacheLoader {

  private Configuration conf;
  private FileSystem fileSystem;
  private Path testDirectory;
  private FileSystemDatasetRepository repo;
  private PartitionStrategy partitionStrategy;
  private FileSystemView<Object> view;

  @Before
  public void setUp() throws IOException {
    this.conf = new Configuration();
    this.fileSystem = FileSystem.get(conf);
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    this.repo = new FileSystemDatasetRepository(conf, testDirectory,
      new EnusrePartitionPathDoesNotExistMetadataProvider(conf, testDirectory));

    partitionStrategy = new PartitionStrategy.Builder()
      .hash("username", 2).build();
    FileSystemDataset<Object> users = (FileSystemDataset<Object>) repo.create(
      "ns", "users",
      new DatasetDescriptor.Builder()
      .schema(USER_SCHEMA)
      .partitionStrategy(partitionStrategy)
      .build());
    view = new FileSystemView<Object>(users, null, null, Object.class);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testInitializeAfterParitionAddedCallback() throws Exception {
    PartitionedDatasetWriter.DatasetWriterCacheLoader<Object> loader
      = new PartitionedDatasetWriter.DatasetWriterCacheLoader<Object>(view);

    StorageKey key = new StorageKey.Builder(partitionStrategy)
      .add("username", "test1")
      .build();
    loader.load(key);
  }

  @Test
  public void testIncrementatlInitializeAfterParitionAddedCallback() throws Exception {
    PartitionedDatasetWriter.IncrementalDatasetWriterCacheLoader<Object> loader
      = new PartitionedDatasetWriter.IncrementalDatasetWriterCacheLoader<Object>(view);

    StorageKey key = new StorageKey.Builder(partitionStrategy)
      .add("username", "test1")
      .build();
    loader.load(key);
  }

  private static class EnusrePartitionPathDoesNotExistMetadataProvider
    extends FileSystemMetadataProvider implements PartitionListener {

    public EnusrePartitionPathDoesNotExistMetadataProvider(Configuration conf, Path rootDirectory) {
      super(conf, rootDirectory);
    }

    @Override
    public void partitionAdded(String namespace, String name, String partition) {
      Path root = getRootDirectory();
      Path partitionPath = new Path(
        FileSystemDatasetRepository.pathForDataset(root, namespace, name),
        partition);
      try {
        Assert.assertFalse("Partition path " + partitionPath + " does not exist",
          getFileSytem().exists(partitionPath));
      } catch (IOException ex) {
        Assert.fail(ex.getMessage());
      }
    }

    @Override
    public void partitionDeleted(String namespace, String name, String partition) {
    }
  }

}
