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

import com.google.common.collect.Sets;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.MetadataProvider;

public class TestFileSystemMetadataProviderBackwardCompatibility {

  private static final Path root = new Path("file:/tmp/tests/mixed-repository");
  private static final Configuration conf = new Configuration();
  private static final DatasetDescriptor descriptor =
      new DatasetDescriptor.Builder()
          .schemaLiteral("\"int\"")
          .build();

  private static FileSystem local;
  private static MetadataProvider provider = null;

  @BeforeClass
  public static void createMixedLayoutRepository() throws IOException {
    local = FileSystem.get(conf);

    local.delete(root, true); // clean any old data

    createMetadata(
        local, new Path(root, new Path("old_1", ".metadata")), "old_1");
    createMetadata(
        local, new Path(root, new Path("old_2", ".metadata")), "old_2");

    provider = new FileSystemMetadataProvider(conf, root);

    provider.create("ns", "new_1", descriptor);
    provider.create("ns2", "new_2", descriptor);
  }

  public static void createMetadata(FileSystem fs, Path path, String name)
      throws IOException {
    fs.mkdirs(path);
    FileSystemMetadataProvider.writeDescriptor(fs, path, name, descriptor);
  }

  @Test
  public void testNamespaces() {
    Assert.assertEquals("Should report default namespace for old datasets",
        Sets.newHashSet("default", "ns", "ns2"),
        Sets.newHashSet(provider.namespaces()));
  }

  @Test
  public void testNamespacesWithOverlap() {
    // use old_1 as a new namespace
    provider.create("old_1", "new_3", descriptor);

    Assert.assertEquals("Should report default namespace for old datasets",
        Sets.newHashSet("old_1", "default", "ns", "ns2"),
        Sets.newHashSet(provider.namespaces()));

    provider.delete("old_1", "new_3");
  }

  @Test
  public void testDatasets() throws IOException {
    Assert.assertEquals("Should report old datasets in default namespace",
        Sets.newHashSet("old_1", "old_2"),
        Sets.newHashSet(provider.datasets("default")));

    provider.create("default", "new_3", descriptor);

    Assert.assertEquals("Should merge old datasets into default namespace",
        Sets.newHashSet("old_1", "old_2", "new_3"),
        Sets.newHashSet(provider.datasets("default")));

    provider.delete("default", "new_3");
  }

  @Test
  public void testDatasetsWithOverlap() {
    // use old_1 as a new namespace
    provider.create("old_1", "new_3", descriptor);

    Assert.assertEquals("Should report old datasets in default namespace",
        Sets.newHashSet("old_1", "old_2"),
        Sets.newHashSet(provider.datasets("default")));

    Assert.assertEquals("Should merge old datasets into default namespace",
        Sets.newHashSet("new_3"),
        Sets.newHashSet(provider.datasets("old_1")));

    provider.delete("old_1", "new_3");
  }

  @Test
  public void testLoad() {
    DatasetDescriptor loaded = provider.load("default", "old_1");
    Assert.assertNotNull("Should find old layout datasets", loaded);
  }

  @Test
  public void testExists() {
    Assert.assertTrue("Should find old layout datasets",
        provider.exists("default", "old_1"));
  }

  @Test
  public void testUpdate() throws IOException {
    DatasetDescriptor updated = new DatasetDescriptor.Builder(descriptor)
        .property("parquet.block.size", "1024")
        .build();

    DatasetDescriptor saved = provider.update("default", "old_2", updated);
    Assert.assertNotNull("Should find saved metadata", saved);
    Assert.assertEquals("Should update old dataset successfully",
        updated.getProperty("parquet.block.size"),
        saved.getProperty("parquet.block.size"));

    DatasetDescriptor loaded = provider.load("default", "old_2");
    Assert.assertNotNull("Should find saved metadata", loaded);
    Assert.assertEquals("Should make changes on disk",
        updated.getProperty("parquet.block.size"),
        loaded.getProperty("parquet.block.size"));

    Assert.assertFalse("Should not move metadata to new location",
        local.exists(new Path(root, new Path("default", "old_2"))));
  }
}
