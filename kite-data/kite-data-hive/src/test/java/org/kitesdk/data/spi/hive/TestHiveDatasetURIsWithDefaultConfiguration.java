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

package org.kitesdk.data.spi.hive;

import java.net.URI;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;

public class TestHiveDatasetURIsWithDefaultConfiguration extends MiniDFSTest {

  private static Configuration existing;
  private static String hdfsAuth;
  private static DatasetDescriptor descriptor;

  @BeforeClass
  public static void createRepositoryAndTestDatasets() throws Exception {
    // set the default configuration so that HDFS is found
    existing = DefaultConfiguration.get();
    DefaultConfiguration.set(getConfiguration());
    hdfsAuth = getDFS().getUri().getAuthority();
    descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
  }

  @AfterClass
  public static void resetDefaultConfiguration() {
    DefaultConfiguration.set(existing);
  }

  @Before
  @After
  public void cleanHive() {
    // ensures all tables are removed
    MetaStoreUtil metastore = new MetaStoreUtil(getConfiguration());
    metastore.dropDatabase("ns", true);
    for (String table : metastore.getAllTables("default")) {
      metastore.dropTable("default", table);
    }
  }

  @Test
  public void testExternal() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive:/tmp/data");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<GenericRecord> ds = Datasets.load("dataset:hive:/tmp/data/ns/test");

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Locations should match",
        URI.create("hdfs://" + hdfsAuth + "/tmp/data/ns/test"),
        ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());

    repo.delete("ns", "test");
  }

  @Test
  public void testExternalRoot() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive:/");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<GenericRecord> ds = Datasets.load("dataset:hive:/ns/test");

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Locations should match",
        URI.create("hdfs://" + hdfsAuth + "/ns/test"),
        ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());

    repo.delete("ns", "test");
  }

  @Test
  public void testExternalRelative() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive:data");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<GenericRecord> ds = Datasets.load("dataset:hive:data/ns/test");

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Path cwd = getDFS().makeQualified(new Path("."));
    Assert.assertEquals("Locations should match",
        new Path(cwd, "data/ns/test").toUri(), ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());

    repo.delete("ns", "test");
  }

  @Test
  public void testManaged() {
    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:hive");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<GenericRecord> ds = Datasets.load("dataset:hive?dataset=test&namespace=ns");

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());

    repo.delete("ns", "test");
  }

  @Test
  public void testMissingDataset() {
    TestHelpers.assertThrows("Should not find dataset: no such dataset",
        DatasetNotFoundException.class, new Runnable() {
      @Override
      public void run() {
        Datasets.load("dataset:hive:/tmp/data/ns/nosuchdataset");
      }
    });
  }

  @Test
  public void testMissingRepository() {
    TestHelpers.assertThrows("Should not find dataset: unknown storage scheme",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.load("dataset:unknown:/tmp/data/ns/test");
          }
        });
  }
}
