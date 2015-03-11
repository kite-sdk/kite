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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;

public class TestHDFSDatasetURIs extends MiniDFSTest {

  private static String hdfsAuth;
  private static DatasetDescriptor descriptor;

  @BeforeClass
  public static void createRepositoryAndTestDatasets() throws Exception {
    hdfsAuth = getDFS().getUri().getAuthority();
    descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:schema/user.avsc")
        .build();
  }

  @Test
  public void testAbsolute() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hdfs://" + hdfsAuth + "/tmp/data");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load("dataset:hdfs://" + hdfsAuth + "/tmp/data/ns/test", Object.class);

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
  public void testAbsoluteTrailingSlash() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hdfs://" + hdfsAuth + "/tmp/data/");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load("dataset:hdfs://" + hdfsAuth + "/tmp/data/ns/test/", Object.class);

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
  public void testAbsoluteRoot() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hdfs://" + hdfsAuth + "/");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load("dataset:hdfs://" + hdfsAuth + "/ns/test",
        Object.class);

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
  public void testRelative() {
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:hdfs://" + hdfsAuth + "/data?absolute=false");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load("dataset:hdfs://" + hdfsAuth + "/data/ns/test?absolute=false",
        Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Path cwd = getDFS().makeQualified(new Path("."));
    Assert.assertEquals("Locations should match",
        new Path(cwd, "data/ns/test").toUri(), ds.getDescriptor().getLocation());
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
    TestHelpers.assertThrows("Should not find dataset: no such dataset",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
                load("dataset:hdfs://" + hdfsAuth + "/tmp/data/nosuchnamespace/test",
                Object.class);
          }
        });
  }

  @Test
  public void testMissingDataset() {
    TestHelpers.assertThrows("Should not find dataset: no such dataset",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
                load("dataset:hdfs://" + hdfsAuth + "/tmp/data/ns/nosuchdataset",
                Object.class);
          }
        });
  }

  @Test
  public void testNotEnoughPathComponents() {
    TestHelpers.assertThrows("Should not match URI pattern",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
                load("dataset:hdfs://" + hdfsAuth + "/test", Object.class);
          }
        });
  }

  @Test
  public void testMissingRepository() {
    TestHelpers.assertThrows("Should not find dataset: unknown storage scheme",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
                load("dataset:unknown://" + hdfsAuth + "/tmp/data/test",
                Object.class);
          }
        });
  }

  @Test
  public void testAbsoluteWebHdfs() {
    Assume.assumeTrue(!Hadoop.isHadoop1());

    String webhdfsAuth = getConfiguration().get(
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    DatasetRepository repo = DatasetRepositories
        .repositoryFor("repo:webhdfs://" + webhdfsAuth + "/tmp/data");
    repo.delete("ns", "test");
    repo.create("ns", "test", descriptor);

    Dataset<Object> ds = Datasets.<Object, Dataset<Object>>
        load("dataset:webhdfs://" + webhdfsAuth + "/tmp/data/ns/test", Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof FileSystemDataset);
    Assert.assertEquals("Locations should match",
        URI.create("webhdfs://" + webhdfsAuth + "/tmp/data/ns/test"),
        ds.getDescriptor().getLocation());
    Assert.assertEquals("Descriptors should match",
        repo.load("ns", "test").getDescriptor(), ds.getDescriptor());
    Assert.assertEquals("Should report correct namespace",
        "ns", ds.getNamespace());
    Assert.assertEquals("Should report correct name",
        "test", ds.getName());

    repo.delete("ns", "test");
  }
}
