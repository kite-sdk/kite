/*
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
package org.kitesdk.data.spi.filesystem;

import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.MetadataProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.BeforeClass;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DatasetRepositories;

public class TestFileSystemRepositoryURIs extends MiniDFSTest {

  @BeforeClass
  public static void loadImpl() {
    new Loader().load();
  }

  @Test
  public void testLocalRelative() throws URISyntaxException {
    URI repositoryUri = new URI("repo:file:target/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.repositoryFor(repositoryUri);

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo",
        repository instanceof FileSystemDatasetRepository);
    MetadataProvider provider = ((FileSystemDatasetRepository) repository)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
        provider instanceof FileSystemMetadataProvider);
    FileSystemMetadataProvider fsProvider = (FileSystemMetadataProvider) provider;
    Assert.assertTrue("FileSystem is a LocalFileSystem",
        fsProvider.getFileSytem() instanceof LocalFileSystem);
    Path expected = fsProvider.getFileSytem().makeQualified(
        new Path("target/dsr-repo-test"));
    Assert.assertEquals("Root directory should be the correct qualified path",
        expected, fsProvider.getRootDirectory());
    Assert.assertEquals("Repository URI scheme", "repo", repository.getUri()
        .getScheme());
    Assert.assertEquals("Repository URI scheme", expected.toUri(),
        new URI(repository.getUri().getSchemeSpecificPart()));
  }

  @Test
  public void testLocalAbsolute() throws URISyntaxException {
    URI repositoryUri = new URI("repo:file:///tmp/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.repositoryFor(repositoryUri);

    FileSystemMetadataProvider provider = (FileSystemMetadataProvider)
        ((FileSystemDatasetRepository) repository).getMetadataProvider();
    Assert.assertEquals("Root directory should be the correct qualified path",
        new Path("file:/tmp/dsr-repo-test"), provider.getRootDirectory());
    Assert.assertEquals("Repository URI", repositoryUri, repository.getUri());
  }

  @Test(expected = DatasetRepositoryException.class)
  public void testHdfsFailsDefault() {
    // the environment doesn't contain the HDFS URI, so this should cause a
    // DatasetRepository exception about not finding HDFS
    DatasetRepositories.repositoryFor("repo:hdfs:/");
  }

  @Test
  public void testHdfsAbsolute() throws URISyntaxException {
    URI hdfsUri = getDFS().getUri();
    URI repositoryUri = new URI("repo:hdfs://" + hdfsUri.getAuthority() + "/tmp/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.repositoryFor(repositoryUri);

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo",
        repository instanceof FileSystemDatasetRepository);
    MetadataProvider provider = ((FileSystemDatasetRepository) repository)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
        provider instanceof FileSystemMetadataProvider);
    FileSystemMetadataProvider fsProvider = (FileSystemMetadataProvider) provider;
    Assert.assertTrue("FileSystem is a DistributedFileSystem",
      fsProvider.getFileSytem() instanceof DistributedFileSystem);
    Path expected = fsProvider.getFileSytem().makeQualified(
        new Path("/tmp/dsr-repo-test"));
    Assert.assertEquals("Root directory should be the correct qualified path",
        expected, fsProvider.getRootDirectory());
    Assert.assertEquals("Repository URI", repositoryUri, repository.getUri());
  }

}
