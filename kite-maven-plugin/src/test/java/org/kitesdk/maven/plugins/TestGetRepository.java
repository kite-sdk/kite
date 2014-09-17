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
package org.kitesdk.maven.plugins;

import java.net.URI;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestGetRepository extends MiniDFSTest {
  private static class TestDatasetMojo extends AbstractDatasetMojo {
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
    }
  }

  private static Configuration original;

  @BeforeClass
  public static void saveOriginalConf() {
    AbstractDatasetMojo.addedConf = false;
    original = DefaultConfiguration.get();
  }

  @AfterClass
  public static void restoreOriginalConf() {
    DefaultConfiguration.set(original);
  }

  @Test
  public void testGetRepositoryFileSystemOldProperty() {
    URI fsUri = getDFS().getUri();

    TestDatasetMojo test = new TestDatasetMojo();
    test.hcatalog = false;
    test.rootDirectory = "/tmp/datasets";
    test.hadoopConfiguration = new Properties();
    test.hadoopConfiguration.setProperty("fs.default.name", fsUri.toString());

    DatasetRepository repo = test.getDatasetRepository();
    Assert.assertNotNull("Should create repo successfully", repo);
    Assert.assertEquals("Should be a HDFS repo",
        "repo:hdfs://" + fsUri.getAuthority() + "/tmp/datasets",
        repo.getUri().toString());
  }

  @Test
  public void testGetRepositoryFileSystemNewProperty() {
    Assume.assumeTrue(!Hadoop.isHadoop1());

    URI fsUri = getDFS().getUri();

    TestDatasetMojo test = new TestDatasetMojo();
    test.hcatalog = false;
    test.rootDirectory = "/tmp/datasets";
    test.hadoopConfiguration = new Properties();
    test.hadoopConfiguration.setProperty("fs.defaultFS", fsUri.toString());

    DatasetRepository repo = test.getDatasetRepository();
    Assert.assertNotNull("Should create repo successfully", repo);
    Assert.assertEquals("Should be a HDFS repo",
        "repo:hdfs://" + fsUri.getAuthority() + "/tmp/datasets",
        repo.getUri().toString());
  }
}
