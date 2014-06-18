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
import com.google.common.io.Resources;
import java.io.File;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.TestHelpers;
import org.slf4j.Logger;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class TestSchemaCommand {

  private static Schema schema = null;
  private Logger console = null;
  private SchemaCommand command;

  @BeforeClass
  public static void parseUserSchema() throws Exception {
    schema = new Schema.Parser().parse(
        Resources.getResource("schema/user.avsc").openStream());
  }

  @Before
  public void setup() throws Exception {
    TestUtil.run("create", "users", "--schema", "resource:schema/user.avsc");
    this.console = mock(Logger.class);
    this.command = new SchemaCommand(console);
    command.setConf(new Configuration());
  }

  @After
  public void removeDatasets() throws Exception {
    TestUtil.run("delete", "users");
    TestUtil.run("delete", "users_2");
  }

  @Test
  public void testSchemaStdout() throws Exception {
    command.datasets = Lists.newArrayList("users");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(schema)));
    verify(console).trace(contains("repo:hive"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMultipleSchemasStdout() throws Exception {
    TestUtil.run("create", "users_2", "--schema", "resource:schema/user.avsc");
    command.datasets = Lists.newArrayList("users", "users_2");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(anyString(),
        eq("users"), argThat(TestUtil.matchesSchema(schema)));
    verify(console).info(anyString(),
        eq("users_2"), argThat(TestUtil.matchesSchema(schema)));
    verify(console).trace(contains("repo:hive"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSchemaToFile() throws Exception {
    command.datasets = Lists.newArrayList("users");
    command.outputPath = "target/user.avsc";
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    String fileContent = Files.toString(
        new File("target/user.avsc"), BaseCommand.UTF8);
    Assert.assertTrue("File should contain pretty printed schema",
        TestUtil.matchesSchema(schema).matches(fileContent));
    verify(console).trace(contains("repo:hive"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMultipleSchemasToFileFails() throws Exception {
    command.datasets = Lists.newArrayList("users", "users_2");
    command.outputPath = "target/user_schemas.avsc";
    TestHelpers.assertThrows("Should reject saving multiple schemas in a file",
        IllegalArgumentException.class, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        command.run();
        return null;
      }
    });
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMinimize() throws Exception {
    command.datasets = Lists.newArrayList("users");
    command.minimize = true;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesMinimizedSchema(schema)));
    verify(console).trace(contains("repo:hive"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingDatasetName() {
    TestHelpers.assertThrows("Should complain when no dataset name is given",
        IllegalArgumentException.class, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        command.run();
        return null;
      }
    });
    verifyZeroInteractions(console);
  }
}
