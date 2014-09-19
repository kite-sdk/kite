/*
 * Copyright 2014 Cloudera Inc.
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
import com.google.common.io.Files;
import java.io.File;
import java.net.URI;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DefaultConfiguration;

import static org.mockito.Mockito.*;
import org.slf4j.Logger;

public class TestFlumeConfigurationCommand {

  private Logger console = null;
  private FlumeConfigCommand command;
  private static Configuration original;
  private static final String DATASET_URI= "dataset:file:target/data/flumeConfig/users";

  @BeforeClass
  public static void setConfiguration() {
    original = DefaultConfiguration.get();
    Configuration conf = new Configuration(original);
    conf.set("fs.defaultFS", "hdfs://nameservice1");
    DefaultConfiguration.set(conf);
  }

  @AfterClass
  public static void restoreConfiguration() {
    DefaultConfiguration.set(original);
  }

  @BeforeClass
  public static void createDataset() throws Exception {
    String avsc = "src/test/resources/test-schemas/user.avsc";

    TestUtil.run("delete", DATASET_URI);
    TestUtil.run("-v", "create", DATASET_URI, "-s", avsc);
  }

  @AfterClass
  public static void deleteDataset() throws Exception {
    TestUtil.run("delete", DATASET_URI);
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new FlumeConfigCommand(console);
    command.setConf(DefaultConfiguration.get());
  }

  @Test
  public void testHdfsUri() throws Exception {
    URI expected = URI.create("repo:hdfs://nameservice1/datasets/ns");
    URI actual = command.getRepoUri(
        URI.create("dataset:hdfs:/datasets/ns/events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testFileUri() throws Exception {
    URI expected = URI.create("repo:file:/datasets/ns");
    URI actual = command.getRepoUri(
        URI.create("dataset:file:/datasets/ns/events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testManagedHiveUri() throws Exception {
    URI expected = URI.create("repo:hive");
    URI actual = command.getRepoUri(
        URI.create("dataset:hive?dataset=events"), "default");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testExternalHiveUri() throws Exception {
    URI expected = URI.create("repo:hive:/datasets/ns?hdfs:host=nameservice1");
    URI actual = command.getRepoUri(
        URI.create("dataset:hive:/datasets/ns/events?namespace=ns&dataset=events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testHBaseUri() throws Exception {
    URI expected = URI.create("repo:hbase:zk1,zk2:2181");
    URI actual = command.getRepoUri(
        URI.create("dataset:hbase:zk1,zk2/events?namespace=ns"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testDatasetRequired() throws Exception {
    final FlumeConfigCommand finalCommand = command;
    TestHelpers.assertThrows("Throw IllegalArgumentException when no dataset is provided",
        IllegalArgumentException.class, new Callable<Integer>() {

          @Override
          public Integer call() throws Exception {
            return finalCommand.run();
          }

        });
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCli() throws Exception {
    int rc = TestUtil.run(console, new Configuration(), "flume-config",
        DATASET_URI);
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDefaults() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testNewFlume() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.newFlume = true;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.dataset.uri = dataset:file:target/data/flumeConfig/users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testAgentName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.agent = "agent";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "agent.sources = avro-log-source\n" +
        "agent.channels = channel-1\n" +
        "agent.sinks = kite-dataset\n" +
        "\n" +
        "agent.sources.avro-log-source.type = avro\n" +
        "agent.sources.avro-log-source.channels = channel-1\n" +
        "agent.sources.avro-log-source.bind = 0.0.0.0\n" +
        "agent.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "agent.channels.channel-1.type = file\n" +
        "\n" +
        "agent.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "agent.sinks.kite-dataset.channel = channel-1\n" +
        "agent.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "agent.sinks.kite-dataset.kite.dataset.name = users\n" +
        "agent.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "agent.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSourceName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.sourceName = "my-source";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = my-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.my-source.type = avro\n" +
        "tier1.sources.my-source.channels = channel-1\n" +
        "tier1.sources.my-source.bind = 0.0.0.0\n" +
        "tier1.sources.my-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testChannelName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.channelName = "my-channel";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = my-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = my-channel\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.my-channel.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = my-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSinkName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.sinkName = "my-sink";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = my-sink\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.my-sink.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.my-sink.channel = channel-1\n" +
        "tier1.sinks.my-sink.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.my-sink.kite.dataset.name = users\n" +
        "tier1.sinks.my-sink.kite.batchSize = 1000\n" +
        "tier1.sinks.my-sink.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomBind() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.bindAddress = "127.0.0.1";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 127.0.0.1\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomPort() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.port = 4242;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 4242\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCheckpointDir() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/flume/checkpoint";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "tier1.channels.channel-1.checkpointDir = /data/flume/checkpoint\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDataDirs() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.dataDirs = Lists.newArrayList("/data/1/flume/data", "/data/2/flume/data");
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "tier1.channels.channel-1.dataDirs = /data/1/flume/data, /data/2/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMemChannel() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.channelType = "memory";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = memory\n" +
        "tier1.channels.channel-1.capacity = 10000000\n" +
        "tier1.channels.channel-1.transactionCapacity = 1000\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCapacity() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.capacity = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "tier1.channels.channel-1.capacity = 42\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testTransactionCapacity() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.transactionCapacity = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "tier1.channels.channel-1.transactionCapacity = 42\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testBatchSize() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.batchSize = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 42\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testRollInterval() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.rollInterval = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 42\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testProxyUser() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.proxyUser = "cloudera";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n" +
        "tier1.sinks.kite-dataset.auth.proxyUser = cloudera\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testOutputPath() throws Exception {
    String outputPath = "target/flumeConfig/flume.properties";
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.outputPath = outputPath;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verifyNoMoreInteractions(console);
    String fileContent = Files.toString(new File(outputPath), BaseCommand.UTF8);
    GenericTestUtils.assertMatches(fileContent,
        "tier1.sources = avro-log-source\n" +
        "tier1.channels = channel-1\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-log-source.type = avro\n" +
        "tier1.sources.avro-log-source.channels = channel-1\n" +
        "tier1.sources.avro-log-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-log-source.port = 41415\n" +
        "\n" +
        "tier1.channels.channel-1.type = file\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = channel-1\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n");
  }
}
