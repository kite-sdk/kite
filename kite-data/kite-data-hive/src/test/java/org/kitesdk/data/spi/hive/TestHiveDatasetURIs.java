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
import org.apache.hadoop.fs.Path;
import org.junit.After;
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
import org.kitesdk.data.spi.filesystem.FileSystemDataset;

public class TestHiveDatasetURIs extends MiniDFSTest {

  private static String hdfsAuth;
  private static String hdfsQueryArgs;
  private static String hdfsQueryArgsOld;
  private static DatasetDescriptor descriptor;

  @BeforeClass
  public static void createRepositoryAndTestDatasets() throws Exception {
    hdfsAuth = getDFS().getUri().getAuthority();
    hdfsQueryArgs = "hdfs:host=" + getDFS().getUri().getHost() +
        "&hdfs:port=" + getDFS().getUri().getPort();
    hdfsQueryArgsOld = "hdfs-host=" + getDFS().getUri().getHost() +
        "&hdfs-port=" + getDFS().getUri().getPort();
    descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
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
  public void testExternal() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive:/tmp/data/ns/test?" + hdfsQueryArgs, Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Locations should match",
        URI.create("hdfs://" + hdfsAuth + "/tmp/data/ns/test"),
        ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "ns", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("ns", "test");
  }

  @Test
  public void testExternalHDFSQueryOptions() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/tmp/data?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive:/tmp/data/ns/test?" + hdfsQueryArgsOld, Object.class);

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
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:/?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive:/ns/test?" + hdfsQueryArgs, Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Locations should match",
        URI.create("hdfs://" + hdfsAuth + "/ns/test"),
        ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "ns", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("ns", "test");
  }

  @Test
  public void testExternalRelative() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive:data?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive:data/ns/test?" + hdfsQueryArgs, Object.class);

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
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive?dataset=test&namespace=ns&" + hdfsQueryArgs, Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "ns", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("ns", "test");
  }

  @Test
  public void testManagedDefaultDatabase() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive?" + hdfsQueryArgs);
    repo.delete("default", "test");
    repo.create("default", "test", descriptor);

    // namespace is not included in the URI
    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive?dataset=test&" + hdfsQueryArgs, Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Descriptors should match",
        repo.load("default", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "default", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("default", "test");
  }

  @Test
  public void testManagedHDFSQueryOptions() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hive?" + hdfsQueryArgs);
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets
        .<Object, Dataset<Object>>load("dataset:hive?dataset=test&namespace=ns&" + hdfsQueryArgsOld, Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "ns", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("ns", "test");
  }

  @Test
  public void testMissingNamespace() {
    TestHelpers.assertThrows("Should not find namespace: no such namespace",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.load("dataset:hive:/tmp/data/nosuchnamespace/nosuchdataset?" + hdfsQueryArgs, Object.class);
          }
        });
  }

  @Test
  public void testMissingDataset() {
    TestHelpers.assertThrows("Should not find dataset: no such dataset",
        DatasetNotFoundException.class, new Runnable() {
      @Override
      public void run() {
        Datasets.load("dataset:hive:/tmp/data/default/nosuchdataset?" + hdfsQueryArgs, Object.class);
      }
    });
  }

  @Test
  public void testExternalNotEnoughPathComponents() {
    TestHelpers.assertThrows("Should not match URI pattern",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.load("dataset:hive:/test", Object.class);
          }
        });
  }

  @Test
  public void testMissingRepository() {
    TestHelpers.assertThrows("Should not find dataset: unknown storage scheme",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Datasets.load("dataset:unknown://" + hdfsAuth + "/tmp/data/test", Object.class);
          }
        });
  }
}
