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
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;
import org.slf4j.Logger;
import org.kitesdk.data.DatasetException;
import static org.mockito.Mockito.*;

public class TestCSVSchemaCommand {

  private static String sample = null;
  private static String failedSample = null;
  private static Schema schema = null;
  private static Schema requiredSchema = null;
  private Logger console = null;
  private CSVSchemaCommand command;

  @BeforeClass
  public static void buildUserSchema() throws Exception {
    sample = "target/users.csv";
    failedSample = "target/users_failed.csv";
    BufferedWriter writer = Files.newWriter(
        new File(sample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id, username, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();

    writer = Files.newWriter(
            new File(failedSample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id, user name, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();


    writer = Files.newWriter(
            new File(failedSample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id, user name, email\n");
    writer.append("1, test, test@example.com\n");
    writer.close();


    schema = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    requiredSchema = SchemaBuilder.record("User").fields()
        .requiredLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new CSVSchemaCommand(console);
    command.setConf(new Configuration());
  }

  @Test
  public void testSchemaStdout() throws Exception {
    command.samplePaths = Lists.newArrayList("target/users.csv");
    command.recordName = "User";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(schema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSchemaRequiredFields() throws Exception {
    command.samplePaths = Lists.newArrayList("target/users.csv");
    command.recordName = "User";
    command.requiredFields = Lists.newArrayList("id");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(requiredSchema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSchemaToFile() throws Exception {
    command.samplePaths = Lists.newArrayList("target/users.csv");
    command.recordName = "User";
    command.outputPath = "target/user.avsc";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/user.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMultipleSamplesFail() throws Exception {
    command.samplePaths = Lists.newArrayList(sample, "target/sample2.csv");
    TestHelpers.assertThrows("Should reject saving multiple schemas in a file",
        IllegalArgumentException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            command.run();
            return null;
          }
        }
    );
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMinimize() throws Exception {
    command.samplePaths = Lists.newArrayList("target/users.csv");
    command.recordName = "User";
    command.minimize = true;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesMinimizedSchema(schema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingSamplePath() {
    TestHelpers.assertThrows("Should complain when no sample csv is given",
        IllegalArgumentException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            command.run();
            return null;
          }
        });
    verifyZeroInteractions(console);
  }

  @Test
  public void testInvalidCSVHeaderFail() throws Exception {
    command.samplePaths = Lists.newArrayList("target/users_failed.csv");
    command.recordName = "User";
    TestHelpers.assertThrows("Should fail when csv header doesn't follow alphanumeric standards",
        DatasetException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
             command.run();
             return null;
          }
        });
    verifyZeroInteractions(console);
  }
}
