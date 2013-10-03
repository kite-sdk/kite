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

package com.cloudera.cdk.data.hcatalog;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.MetadataProvider;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.filesystem.TestFileSystemURIs;
import java.net.URI;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This verifies that the URI system returns correctly configured repositories.
 *
 * This inherits from TestFileSystemURIs because those URIs should continue to
 * work correctly after the Hive URIs are added.
 */
public class TestHiveURIs extends TestFileSystemURIs {

  @BeforeClass
  public static void registerURIs() {
    new com.cloudera.cdk.data.filesystem.impl.Loader().load();
    new com.cloudera.cdk.data.hcatalog.impl.Loader().load();
  }

  @Test
  public void testManagedURI() {
    DatasetRepository repo = DatasetRepositories.open("repo:hive");

    Assert.assertNotNull("Received a repository", repo);
    Assert.assertTrue("Repo should be a HCatalogDatasetRepository",
        repo instanceof HCatalogDatasetRepository);
    MetadataProvider provider = ((HCatalogDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a HCatalogManagedMetadataProvider",
        provider instanceof HCatalogManagedMetadataProvider);
  }

  @Test(expected=DatasetRepositoryException.class)
  public void testExternalURIFailsWithoutHDFSInfo() {
    DatasetRepositories.open("repo:hive:/tmp/hive-repo");

    // if no HDFS connection options are given, then the constructed URI will
    // look like this: hdfs:/tmp/hive-repo, but without the HDFS host and port,
    // this will fail.
  }

  @Test
  public void testExternalURI() {
    URI hdfsUri = getDFS().getUri();
    DatasetRepository repo = DatasetRepositories.open(
        "repo:hive:/tmp/hive-repo?hdfs-host=" + hdfsUri.getHost() +
        "&hdfs-port=" + hdfsUri.getPort());

    Assert.assertNotNull("Received a repository", repo);
    org.junit.Assert.assertTrue("Repo is a FileSystem repo",
        repo instanceof FileSystemDatasetRepository);
    MetadataProvider provider = ((FileSystemDatasetRepository) repo)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a HCatalogExternalMetadataProvider",
        provider instanceof HCatalogExternalMetadataProvider);

    // verify location
    DatasetDescriptor created = provider.create("test",
        new DatasetDescriptor.Builder()
        .schemaLiteral("\"string\"")
        .get());
    Assert.assertEquals("Locatoin should be in HDFS",
        "hdfs", created.getLocation().getScheme());
    Assert.assertEquals("Location should have the correct HDFS host",
        hdfsUri.getHost(), created.getLocation().getHost());
    Assert.assertEquals("Location should have the correct HDFS port",
        hdfsUri.getPort(), created.getLocation().getPort());
    Assert.assertTrue("Location should be in the repo path",
        created.getLocation().getPath().startsWith("/tmp/hive-repo"));
  }
}
