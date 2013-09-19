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

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.TestMetadataProviders;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFileSystemMetadataProvider extends TestMetadataProviders {

  private FileSystem fileSystem;
  private Path testDirectory;

  @Override
  public MetadataProvider newProvider(Configuration conf) {
    this.testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    return new FileSystemMetadataProvider(conf, testDirectory);
  }

  @Before
  public void before() throws IOException {
    this.fileSystem = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
    testDirectory = null;
  }

  @Test
  public void testCreateMetadataFiles() throws IOException {
    ensureCreated();

    Path namedDirectory = new Path(testDirectory, NAME);
    Path metadataDirectory = new Path(namedDirectory, ".metadata");
    Path propertiesFile = new Path(metadataDirectory, "descriptor.properties");
    Path schemaFile = new Path(metadataDirectory, "schema.avsc");

    Assert.assertTrue("Named directory should exist for name:" + NAME,
        fileSystem.exists(namedDirectory));
    Assert.assertTrue("Metadata directory should exist",
        fileSystem.exists(metadataDirectory));
    Assert.assertTrue("Descriptor properties file should exist", 
        fileSystem.exists(propertiesFile));
    Assert.assertTrue("Descriptor schema file should exist",
        fileSystem.exists(schemaFile));
  }

  @Test
  public void testDeleteRemovesMetadataFiles() throws IOException {
    testCreateMetadataFiles();

    DatasetDescriptor loaded = provider.load(NAME);

    Path namedDirectory = new Path(loaded.getLocation());
    Path metadataDirectory = new Path(namedDirectory, ".metadata");
    Path propertiesFile = new Path(metadataDirectory, "descriptor.properties");
    Path schemaFile = new Path(metadataDirectory, "schema.avsc");

    boolean result = provider.delete(NAME);
    Assert.assertTrue(result);
    Assert.assertFalse("Descriptor properties file should not exist",
        fileSystem.exists(propertiesFile));
    Assert.assertFalse("Descriptor schema file should not exist",
        fileSystem.exists(schemaFile));
    Assert.assertFalse("Metadata directory should not exist",
        fileSystem.exists(metadataDirectory));
    Assert.assertFalse("Named directory should note exist for name:" + NAME,
        fileSystem.exists(namedDirectory));
  }

}
