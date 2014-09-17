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

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestCreateDataset extends MiniDFSTest {

  private static final String DATASET_URI = "dataset:hdfs:/tmp/data/ns/users";
  private static Properties dfsProps = null;

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

  @BeforeClass
  public static void setDFSProps() {
    dfsProps = new Properties();
    System.err.println("Using Hadoop FS property: " +
        (Hadoop.isHadoop1() ? "fs.default.name" : "fs.defaultFS"));
    dfsProps.setProperty(
        Hadoop.isHadoop1() ? "fs.default.name" : "fs.defaultFS",
        getDFS().getUri().toString());
  }

  @Test
  public void testCreateWithDatasetURI() throws Exception {
    try {
      CreateDatasetMojo mojo = new CreateDatasetMojo();
      mojo.hadoopConfiguration = dfsProps;
      mojo.avroSchemaFile = "schema/user.avsc";
      mojo.uri = DATASET_URI;

      mojo.execute();

      Assert.assertTrue("Dataset should exist", Datasets.exists(DATASET_URI));
    } finally {
      Datasets.delete(DATASET_URI);
    }
  }

  @Test
  public void testCreateWithRepositoryURI() throws Exception {
    try {
      CreateDatasetMojo mojo = new CreateDatasetMojo();
      mojo.hadoopConfiguration = dfsProps;
      mojo.avroSchemaFile = "schema/user.avsc";
      mojo.repositoryUri = "repo:hdfs:/tmp/data";
      mojo.datasetNamespace = "ns";
      mojo.datasetName = "users";

      mojo.execute();

      Assert.assertTrue("Dataset should exist", Datasets.exists(DATASET_URI));
    } finally {
      Datasets.delete(DATASET_URI);
    }
  }
}
