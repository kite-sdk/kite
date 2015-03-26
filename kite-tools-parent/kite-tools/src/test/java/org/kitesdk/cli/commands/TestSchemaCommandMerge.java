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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestSchemaCommandMerge {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static Schema userSchema;
  private static Schema userSchemaUpdate;
  private File schemaFile;
  private Logger console = null;
  private SchemaCommand command;

  @BeforeClass
  public static void parseUserSchema() throws Exception {
    userSchema = new Schema.Parser().parse(
        Resources.getResource("schema/user.avsc").openStream());
    userSchemaUpdate = SchemaBuilder.record("user").fields()
        .requiredLong("id")
        .requiredString("username")
        .requiredString("email")
        .endRecord();
  }

  @Before
  public void setup() throws Exception {
    schemaFile = temp.newFile("user_v2.avsc").getAbsoluteFile();
    Files.write(userSchemaUpdate.toString(), schemaFile, Charset.forName("utf8"));

    TestUtil.run("create", "users", "--schema", "resource:schema/user.avsc");
    this.console = mock(Logger.class);
    this.command = new SchemaCommand(console);
    command.merge = true;
    command.setConf(new Configuration());
  }

  @After
  public void removeDatasets() throws Exception {
    TestUtil.run("delete", "users");

    schemaFile.delete();
  }

  @Test
  public void testSingleSchemaDatasetURI() throws Exception {
    command.datasets = Lists.newArrayList("users");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(userSchema)));
    verify(console).trace(contains("repo:hive"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSingleSchemaResourceURI() throws Exception {
    command.datasets = Lists.newArrayList("resource:schema/user.avsc");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(userSchema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSingleSchemaFile() throws Exception {
    command.datasets = Lists.newArrayList(schemaFile.toString());
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(userSchemaUpdate)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSchemaMerge() throws Exception {
    command.datasets = Lists.newArrayList(
        schemaFile.toString(),
        "resource:schema/user.avsc");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);

    Schema merged = SchemaBuilder.record("user").fields()
        .optionalLong("id") // required in one, missing in the other
        .requiredString("username")
        .requiredString("email")
        .endRecord();

    verify(console).info(argThat(TestUtil.matchesSchema(merged)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testIncompatibleMerge() throws Exception {
    command.datasets = Lists.newArrayList(
        "resource:schema/string.avsc",
        "resource:schema/user.avsc");

    TestHelpers.assertThrows(
        "Should reject incompatible schemas, not produce a union schema",
        IncompatibleSchemaException.class, new Callable() {
      @Override
      public Integer call() throws IOException {
        return command.run();
      }
    });
  }
}
