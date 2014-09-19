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
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;

import static org.mockito.Mockito.*;
import org.slf4j.Logger;

public class TestLoggingConfigurationCommand {

  private Logger console = null;
  private LoggingConfigCommand command;
  private static final String DATASET_URI= "dataset:file:target/data/logConfig/users";

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
    this.command = new LoggingConfigCommand(console);
    command.setConf(new Configuration());
  }

  @Test
  public void testCli() throws Exception {
    int rc = TestUtil.run(console, new Configuration(), "logging-config", "--host",
        "quickstart.cloudera", "--package", "org.kitesdk.test.logging",
        DATASET_URI);
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schema.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDefaults() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verify(console).info(matches(
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schema.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testLogAll() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
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
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schema.avsc\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCustomPort() throws Exception {
    command.datasetName = Lists.newArrayList(DATASET_URI);
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
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schema.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testOutputPath() throws Exception {
    String outputPath = "target/logConfig/log4j.properties";
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    command.outputPath = outputPath;
    int rc = command.run();
    Assert.assertEquals("Return code should be 0", 0, rc);
    verifyNoMoreInteractions(console);
    String fileContent = Files.toString(new File(outputPath), BaseCommand.UTF8);
    GenericTestUtils.assertMatches(fileContent,
        "log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender\n" +
        "log4j.appender.flume.Hostname = quickstart.cloudera\n" +
        "log4j.appender.flume.Port = 41415\n" +
        "log4j.appender.flume.UnsafeMode = true\n" +
        "log4j.appender.flume.AvroSchemaUrl = file:.*/target/data/logConfig/users/.metadata/schema.avsc\n" +
        "\n" +
        "# Log events from the following Java class/package:\n" +
        "log4j.logger.org.kitesdk.test.logging = INFO, flume\n");
  }

  @Test
  public void testDatasetRequired() throws Exception {
    command.hostname = "quickstart.cloudera";
    command.packageName = "org.kitesdk.test.logging";
    final LoggingConfigCommand finalCommand = command;
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
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.packageName = "org.kitesdk.test.logging";
    final LoggingConfigCommand finalCommand = command;
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
    command.datasetName = Lists.newArrayList(DATASET_URI);
    command.hostname = "quickstart.cloudera";
    final LoggingConfigCommand finalCommand = command;
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
