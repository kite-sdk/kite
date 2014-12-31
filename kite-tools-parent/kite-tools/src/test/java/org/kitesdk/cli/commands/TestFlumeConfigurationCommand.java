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
import org.apache.hadoop.hbase.HConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;
import org.kitesdk.data.spi.DefaultConfiguration;

import static org.mockito.Mockito.*;
import org.slf4j.Logger;

public class TestFlumeConfigurationCommand {

  private Logger console = null;
  private FlumeConfigCommand command;
  private static Configuration original;
  private static String zkQuorum;
  private static String zkPort;
  private static String hdfsHost;
  private static String hdfsPort;
  private static final String DATASET_URI= "dataset:file:target/data/flumeConfig/users";
  private static boolean hdfsIsDefault = false;

  @BeforeClass
  public static void setConfiguration() throws Exception {
    HBaseTestUtils.getMiniCluster();

    original = DefaultConfiguration.get();
    Configuration conf = HBaseTestUtils.getConf();
    DefaultConfiguration.set(conf);

    zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
    zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);

    URI defaultFs = URI.create(conf.get("fs.default.name"));
    hdfsIsDefault = "hdfs".equals(defaultFs.getScheme());
    hdfsHost = defaultFs.getHost();
    hdfsPort = Integer.toString(defaultFs.getPort());
  }

  @AfterClass
  public static void restoreConfiguration() throws Exception {
    DefaultConfiguration.set(original);
    HBaseTestUtils.util.shutdownMiniCluster();
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
    Assume.assumeTrue(hdfsIsDefault);
    URI expected = URI.create("repo:hdfs://" + hdfsHost + ":" + hdfsPort + "/datasets/ns");
    URI actual = command.getLegacyRepoUri(
        URI.create("dataset:hdfs:/datasets/ns/events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testFileUri() throws Exception {
    URI expected = URI.create("repo:file:/datasets/ns");
    URI actual = command.getLegacyRepoUri(
        URI.create("dataset:file:/datasets/ns/events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testManagedHiveUri() throws Exception {
    URI expected = URI.create("repo:hive");
    URI actual = command.getLegacyRepoUri(
        URI.create("dataset:hive?dataset=events"), "default");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testExternalHiveUri() throws Exception {
    Assume.assumeTrue(hdfsIsDefault);
    URI expected = URI.create("repo:hive:/datasets/ns?hdfs:host="+hdfsHost+"&hdfs:port="+hdfsPort);
    URI actual = command.getLegacyRepoUri(
        URI.create("dataset:hive:/datasets/ns/events?namespace=ns&dataset=events"), "ns");
    Assert.assertEquals("Unexpected repository URI", expected, actual);
  }

  @Test
  public void testHBaseUri() throws Exception {
    URI expected = URI.create("repo:hbase:"+zkQuorum+":"+zkPort);
    URI actual = command.getLegacyRepoUri(
        URI.create("dataset:hbase:" + zkQuorum + ":" + zkPort +
            "/events?namespace=ns"), "ns");
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
  public void testCheckpointAndDataDirRequired() throws Exception {
    final FlumeConfigCommand finalCommand = command;
    finalCommand.datasetName = Lists.newArrayList(DATASET_URI);
    TestHelpers.assertThrows("Throw IllegalArgumentException when no checkpoint and data dirs are provided",
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
    int rc = TestUtil.run(console, "flume-config",
        "--checkpoint-dir", "/data/0/flume/checkpoint", "--data-dir",
        "/data/1/flume/data", DATASET_URI);
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCliDataDirs() throws Exception {
    int rc = TestUtil.run(console, "flume-config",
        "--checkpoint-dir", "/data/0/flume/checkpoint",
        "--data-dir", "/data/1/flume/data", "--data-dir", "/data/2/flume/data",
        DATASET_URI);
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data, /data/2/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDefaults() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testNewFlume() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.newFlume = true;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.dataset.uri = dataset:file:target/data/flumeConfig/users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testAgentName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.agent = "agent";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "agent.sources = avro-event-source\n" +
        "agent.channels = avro-event-channel\n" +
        "agent.sinks = kite-dataset\n" +
        "\n" +
        "agent.sources.avro-event-source.type = avro\n" +
        "agent.sources.avro-event-source.channels = avro-event-channel\n" +
        "agent.sources.avro-event-source.bind = 0.0.0.0\n" +
        "agent.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "agent.channels.avro-event-channel.type = file\n" +
        "agent.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "agent.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "agent.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "agent.sinks.kite-dataset.channel = avro-event-channel\n" +
        "agent.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "agent.sinks.kite-dataset.kite.dataset.name = users\n" +
        "agent.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "agent.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSourceName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.sourceName = "my-source";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = my-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.my-source.type = avro\n" +
        "tier1.sources.my-source.channels = avro-event-channel\n" +
        "tier1.sources.my-source.bind = 0.0.0.0\n" +
        "tier1.sources.my-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testChannelName() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.channelName = "my-channel";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = my-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = my-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.my-channel.type = file\n" +
        "tier1.channels.my-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.my-channel.dataDirs = /data/1/flume/data\n" +
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
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.sinkName = "my-sink";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = my-sink\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.my-sink.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.my-sink.channel = avro-event-channel\n" +
        "tier1.sinks.my-sink.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.my-sink.kite.dataset.name = users\n" +
        "tier1.sinks.my-sink.kite.batchSize = 1000\n" +
        "tier1.sinks.my-sink.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomBind() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.bindAddress = "127.0.0.1";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 127.0.0.1\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomPort() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.port = 4242;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 4242\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDataDirs() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data", "/data/2/flume/data");
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data, /data/2/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
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
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = memory\n" +
        "tier1.channels.avro-event-channel.capacity = 10000000\n" +
        "tier1.channels.avro-event-channel.transactionCapacity = 1000\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCapacity() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.capacity = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.capacity = 42\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testTransactionCapacity() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.transactionCapacity = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.transactionCapacity = 42\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testBatchSize() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.batchSize = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 42\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testRollInterval() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.rollInterval = 42;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 42\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testProxyUser() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.proxyUser = "cloudera";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
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
    command.checkpointDir = "/data/0/flume/checkpoint";
    command.dataDirs = Lists.newArrayList("/data/1/flume/data");
    command.outputPath = outputPath;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verifyNoMoreInteractions(console);
    String fileContent = Files.toString(new File(outputPath), BaseCommand.UTF8);
    TestUtil.assertMatches(
        "tier1.sources = avro-event-source\n" +
        "tier1.channels = avro-event-channel\n" +
        "tier1.sinks = kite-dataset\n" +
        "\n" +
        "tier1.sources.avro-event-source.type = avro\n" +
        "tier1.sources.avro-event-source.channels = avro-event-channel\n" +
        "tier1.sources.avro-event-source.bind = 0.0.0.0\n" +
        "tier1.sources.avro-event-source.port = 41415\n" +
        "\n" +
        "tier1.channels.avro-event-channel.type = file\n" +
        "tier1.channels.avro-event-channel.checkpointDir = /data/0/flume/checkpoint\n" +
        "\n" +
        "# A list of directories where Flume will persist records that are waiting to be\n" +
        "# processed by the sink. You can use multiple directories on different physical\n" +
        "# disks to increase throughput.\n" +
        "tier1.channels.avro-event-channel.dataDirs = /data/1/flume/data\n" +
        "\n" +
        "tier1.sinks.kite-dataset.type = org.apache.flume.sink.kite.DatasetSink\n" +
        "tier1.sinks.kite-dataset.channel = avro-event-channel\n" +
        "tier1.sinks.kite-dataset.kite.repo.uri = repo:file:.*/target/data/flumeConfig\n" +
        "tier1.sinks.kite-dataset.kite.dataset.name = users\n" +
        "tier1.sinks.kite-dataset.kite.batchSize = 1000\n" +
        "tier1.sinks.kite-dataset.kite.rollInterval = 30\n", fileContent);
  }
}
