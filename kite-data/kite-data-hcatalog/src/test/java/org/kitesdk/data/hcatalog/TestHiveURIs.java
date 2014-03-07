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

import org.kitesdk.data.filesystem.TestFileSystemURIs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.MetadataProvider;
import java.net.URI;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.hcatalog.impl.HCatalog;

/**
 * This verifies that the URI system returns correctly configured repositories.
 *
 * This inherits from TestFileSystemURIs because those URIs should continue to
 * work correctly after the Hive URIs are added.
 */
public class TestHiveURIs extends TestFileSystemURIs {

  @BeforeClass
  public static void registerURIs() {
    new org.kitesdk.data.filesystem.impl.Loader().load();
    new org.kitesdk.data.hcatalog.impl.Loader().load();
  }

  @After
  public void cleanHCatalog() {
    // ensures all tables are removed
    HCatalog hcat = new HCatalog(new Configuration());
    for (String tableName : hcat.getAllTables("default")) {
      hcat.dropTable("default", tableName);
    }
  }

  @Test
  public void testManagedURI() throws Exception {
    DatasetRepository repo = DatasetRepositories.open("repo:hive");

    Assert.assertNotNull("Received a repository", repo);
    Assert.assertTrue("Repo should be a HCatalogDatasetRepository",
        repo instanceof HCatalogDatasetRepository);
    MetadataProvider provider = ((HCatalogDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo should be using a HCatalogManagedMetadataProvider",
        provider instanceof HCatalogManagedMetadataProvider);
    Assert.assertEquals("Repository URI", new URI("repo:hive"), repo.getUri());
  }

  @Test
  public void testManagedURIWithHostAndPort() {
    DatasetRepository repo = DatasetRepositories.open("repo:hive://meta-host:1234");
    Assert.assertNotNull("Received a repository", repo);
    Assert.assertTrue("Repo should be a HCatalogDatasetRepository",
        repo instanceof HCatalogDatasetRepository);
    MetadataProvider provider = ((HCatalogDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo should be using a HCatalogManagedMetadataProvider",
        provider instanceof HCatalogManagedMetadataProvider);
    Assert.assertEquals("Repository URI", URI.create("repo:hive://meta-host:1234"),
        repo.getUri());
  }

  @Test(expected=DatasetRepositoryException.class)
  public void testExternalURIFailsWithoutHDFSInfo() {
    DatasetRepositories.open("repo:hive:/tmp/hive-repo");

    // if no HDFS connection options are given, then the constructed URI will
    // look like this: hdfs:/tmp/hive-repo, but without the HDFS host and port,
    // this will fail.
  }

  @Test(expected=DatasetRepositoryException.class)
  public void testExternalOpaqueURIFailsWithoutHDFSInfo() {
    DatasetRepositories.open("repo:hive:tmp/hive-repo");

    // if no HDFS connection options are given, then the constructed URI will
    // look like this: hdfs:/tmp/hive-repo, but without the HDFS host and port,
    // this will fail.
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testExternalURI() {
    URI hdfsUri = getDFS().getUri();
    URI repoUri = URI.create("repo:hive:/tmp/hive-repo?hdfs-host=" + hdfsUri.getHost() +
        "&hdfs-port=" + hdfsUri.getPort());
    DatasetRepository repo = DatasetRepositories.open(repoUri);

    Assert.assertNotNull("Received a repository", repo);
    org.junit.Assert.assertTrue("Repo should be a HCatalogExternalDatasetRepository",
        repo instanceof HCatalogExternalDatasetRepository);
    Assert.assertEquals("Repository URI", repoUri, repo.getUri());

    // verify location
    DatasetDescriptor created = repo.create("test",
        new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .build()).getDescriptor();
    Assert.assertEquals("Location should be in HDFS",
        "hdfs", created.getLocation().getScheme());
    Assert.assertEquals("Location should have the correct HDFS host",
        hdfsUri.getHost(), created.getLocation().getHost());
    Assert.assertEquals("Location should have the correct HDFS port",
        hdfsUri.getPort(), created.getLocation().getPort());
    Assert.assertTrue("Location should be in the repo path",
        created.getLocation().getPath().startsWith("/tmp/hive-repo"));
  }

  @Test
  public void testExternalURIWithHostAndPort() {
    URI hdfsUri = getDFS().getUri();
    URI repoUri = URI.create("repo:hive://meta-host:1234/tmp/data?hdfs-host=" +
        hdfsUri.getHost() + "&hdfs-port=" + hdfsUri.getPort());
    DatasetRepository repo = DatasetRepositories.open(repoUri);

    Assert.assertNotNull("Received a repository", repo);
    org.junit.Assert.assertTrue("Repo should be a HCatalogExternalDatasetRepository",
        repo instanceof HCatalogExternalDatasetRepository);
    Assert.assertEquals("Repository URI", repoUri, repo.getUri());
  }

  @Test
  public void testExternalURIWithRootPath() {
    URI hdfsUri = getDFS().getUri();
    URI repoUri = URI.create("repo:hive:/?hdfs-host=" + hdfsUri.getHost() +
        "&hdfs-port=" + hdfsUri.getPort());
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    Assert.assertNotNull("Received a repository", repo);
    Assert.assertTrue("Repo should be a HCatalogExternalDatasetRepository",
        repo instanceof HCatalogExternalDatasetRepository);
    MetadataProvider provider = ((HCatalogExternalDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo should be using a HCatalogExternalMetadataProvider",
        provider instanceof HCatalogExternalMetadataProvider);
    Assert.assertEquals("Repository URI", repoUri, repo.getUri());
  }

  @Test
  public void testExternalURIWithHostAndPortAndRootPath() {
    URI hdfsUri = getDFS().getUri();
    URI repoUri = URI.create("repo:hive://meta-host:1234/?hdfs-host=" +
        hdfsUri.getHost() + "&hdfs-port=" + hdfsUri.getPort());
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    Assert.assertNotNull("Received a repository", repo);
    Assert.assertTrue("Repo should be a HCatalogExternalDatasetRepository",
        repo instanceof HCatalogExternalDatasetRepository);
    MetadataProvider provider = ((HCatalogExternalDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo should be using a HCatalogManagedMetadataProvider",
        provider instanceof HCatalogExternalMetadataProvider);
    Assert.assertEquals("Repository URI", repoUri, repo.getUri());
  }
}
