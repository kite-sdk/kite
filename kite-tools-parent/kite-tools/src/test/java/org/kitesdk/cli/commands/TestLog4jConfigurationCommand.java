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
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;

import static org.mockito.Mockito.*;
import org.slf4j.Logger;

public class TestLog4jConfigurationCommand {

  private Logger console = null;
  private Log4jConfigCommand command;
  private static final String FILE_DATASET_URI = "dataset:file:target/data/logConfig/users";
  private static final String HIVE_DATASET_NAME = "users";

  @BeforeClass
  public static void createDatasets() throws Exception {
    String avsc = "resource:test-schemas/user.avsc";

    TestUtil.run("delete", FILE_DATASET_URI);
    TestUtil.run("-v", "create", FILE_DATASET_URI, "-s", avsc);

    TestUtil.run("delete", HIVE_DATASET_NAME);
    TestUtil.run("-v", "create", HIVE_DATASET_NAME, "-s", avsc);
  }

  @AfterClass
  public static void deleteDatasets() throws Exception {
    TestUtil.run("delete", FILE_DATASET_URI);
    TestUtil.run("delete", HIVE_DATASET_NAME);
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new Log4jConfigCommand(console);
    command.setConf(new Configuration());
  }

  @Test
  public void testCli() throws Exception {
    int rc = TestUtil.run(console, new Configuration(), "log4j-config", "--host",
        "quickstart.cloudera", "--package", "org.kitesdk.test.logging",
        FILE_DATASET_URI);
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schemas/1.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCliHive() throws Exception {
    int rc = TestUtil.run(console, new Configuration(), "log4j-config", "--host",
        "quickstart.cloudera", "--package", "org.kitesdk.test.logging",
        "users");
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).trace(startsWith("Repository URI:"));
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verify(console).warn("Warning: The dataset {} does not have a schema URL. The schema will be sent with each event.", "users");
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDefaults() throws Exception {
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schemas/1.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testHiveDataset() throws Exception {
    command.datasetName = Lists.newArrayList("users");
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).trace(startsWith("Repository URI:"));
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verify(console).warn("Warning: The dataset {} does not have a schema URL. The schema will be sent with each event.", "users");
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testLogAll() throws Exception {
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.logAll = true;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "# Log events from all classes:\n" +
        "log4j.rootLogger = INFO, flume\n" +
        "\n" +
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schemas/1.avsc\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomPort() throws Exception {
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    command.port = 4242;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 4242\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schemas/1.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testOutputPath() throws Exception {
    String outputPath = "target/logConfig/log4j.properties";
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    command.outputPath = outputPath;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verifyNoMoreInteractions(console);
    String fileContent = Files.toString(new File(outputPath), BaseCommand.UTF8);
    TestUtil.assertMatches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schemas/1.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n", fileContent);
  }

  @Test
  public void testDatasetRequired() throws Exception {
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    final Log4jConfigCommand finalCommand = command;
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
  public void testHostnameRequired() throws Exception {
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.packageName = "org.kitesdk.test.logging";
    final Log4jConfigCommand finalCommand = command;
    TestHelpers.assertThrows("Throw IllegalArgumentException when no hostname is provided",
        IllegalArgumentException.class, new Callable<Integer>() {

          @Override
          public Integer call() throws Exception {
            return finalCommand.run();
          }

        });
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testPackageNameOrLogAllRequired() throws Exception {
    command.datasetName = Lists.newArrayList(FILE_DATASET_URI);
    command.hostname = "quickstart.cloudera";
    final Log4jConfigCommand finalCommand = command;
    TestHelpers.assertThrows("Throw IllegalArgumentException when package name and log all are not provided",
        IllegalArgumentException.class, new Callable<Integer>() {

          @Override
          public Integer call() throws Exception {
            return finalCommand.run();
          }

        });
    verifyNoMoreInteractions(console);
  }
}
