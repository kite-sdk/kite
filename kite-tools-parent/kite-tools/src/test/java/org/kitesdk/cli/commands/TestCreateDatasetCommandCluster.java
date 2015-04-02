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
package org.kitesdk.cli.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.example.User;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;
import org.kitesdk.data.spi.hive.MetaStoreUtil;
import org.slf4j.Logger;

import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestCreateDatasetCommandCluster extends MiniDFSTest {

  private static final AtomicInteger ids = new AtomicInteger(0);
  private static final Map<String, DatasetRepository> repos = Maps.newHashMap();
  private String id = null;
  private CreateDatasetCommand command = null;
  private Logger console;

  @BeforeClass
  public static void addMockRepoBuilder() throws Exception {
    Registration.register(
        new URIPattern("mock::id"), new URIPattern("mock::id"),
        new OptionBuilder<DatasetRepository>() {
          @Override
          public DatasetRepository getFromOptions(Map<String, String> options) {
            DatasetRepository repo = mock(DatasetRepository.class);
            repos.put(options.get("id"), repo);
            return repo;
          }
        }
    );
  }

  @Before
  public void setUp() {
    this.id = Integer.toString(ids.addAndGet(1));
    this.console = mock(Logger.class);
    this.command = new CreateDatasetCommand(console);
    this.command.setConf(getConfiguration());
    this.command.repoURI = "repo:mock:" + id;
  }

  public DatasetRepository getMockRepo() {
    return repos.get(id);
  }

  @Test
  public void testBasicUse() throws Exception {
    command.avroSchemaFile = "resource:test-schemas/user.avsc";
    command.datasets = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("default", "users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testBasicUseLocalSchema() throws Exception {
    String avsc = "target/localUser.avsc";
    FSDataOutputStream out = getFS()
        .create(new Path(avsc), true /* overwrite */ );
    ByteStreams.copy(Resources.getResource("test-schemas/user.avsc").openStream(), out);
    out.close();
    command.avroSchemaFile = avsc;
    command.datasets = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("default", "users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testBasicUseHDFSSchema() throws Exception {
    // this test needs to set the default configuration so that the
    // DatasetDescriptor can resolve HDFS to qualify the path. otherwise,
    // the default FS is file:/ and the avsc path is not qualified, causing
    // an IOException when it tries to read the file. Setting up HDFS correctly
    // in the environment fixes the problem.
    Configuration existing = DefaultConfiguration.get();
    DefaultConfiguration.set(getConfiguration());

    String avsc = "hdfs:/tmp/schemas/hdfsUser.avsc";
    FSDataOutputStream out = getDFS()
        .create(new Path(avsc), true /* overwrite */ );
    ByteStreams.copy(Resources.getResource("test-schemas/user.avsc").openStream(), out);
    out.close();
    command.avroSchemaFile = avsc;
    command.datasets = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(getMockRepo()).create("default", "users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));

    // restore the previous Configuration
    DefaultConfiguration.set(existing);
  }

  @Test
  public void testCreateWithExistingDataPartitions() throws IOException {
    Configuration existing = DefaultConfiguration.get();
    try {
      DefaultConfiguration.set(getConfiguration());

      // create a partitioned dataset and add a record
      DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
          .schema(User.class)
          .partitionStrategy(new PartitionStrategy.Builder()
              .hash("username", 4)
              .build())
          .build();
      Dataset<User> users = Datasets.create(
          "dataset:hdfs:/tmp/datasets/users", descriptor, User.class);
      DatasetWriter<User> writer = null;
      try {
        writer = users.newWriter();
        writer.write(new User("test", "test@example.com"));
      } finally {
        Closeables.closeQuietly(writer);
      }

      // remove the dataset's metadata
      getDFS().delete(new Path("/tmp/datasets/users/.metadata"), true);

      // use the create command to create an external table in Hive
      Logger console = mock(Logger.class);
      CreateDatasetCommand create = new CreateDatasetCommand(console);
      create.setConf(getConfiguration());
      create.datasets = Lists.newArrayList("dataset:hive:users");
      create.location = "hdfs:/tmp/datasets/users";
      create.run();

      // validate the dataset
      Dataset<GenericRecord> loaded = Datasets.load("dataset:hive:users");
      Assert.assertNotNull("Should successfully create Hive dataset", loaded);
      Assert.assertTrue("Should be partitioned",
          loaded.getDescriptor().isPartitioned());
      PartitionStrategy expectedStrategy = new PartitionStrategy.Builder()
          .provided("username_hash", "int")
          .build();
      Assert.assertEquals("Should have a provided partition strategy",
          expectedStrategy, loaded.getDescriptor().getPartitionStrategy());

      MetaStoreUtil meta = new MetaStoreUtil(getConfiguration());
      List<String> partitions = meta.listPartitions("default", "users", (short) 10);
      Assert.assertEquals("Table should have a partition",
          1, partitions.size());
      Assert.assertTrue("Partition should exist",
          getDFS().exists(new Path(partitions.get(0))));
      Assert.assertTrue("Partition should be a partition directory",
          partitions.get(0).contains("/tmp/datasets/users/username_hash="));

    } finally {
      Datasets.delete("dataset:hive:users");
      DefaultConfiguration.set(existing);
    }
  }

}
