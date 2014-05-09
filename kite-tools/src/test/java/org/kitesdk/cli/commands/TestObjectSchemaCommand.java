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
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.cli.example.User;
import org.kitesdk.data.TestHelpers;
import org.mockito.Matchers;
import org.slf4j.Logger;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestObjectSchemaCommand {

  private static Schema schema = null;
  private Logger console = null;
  private ObjectSchemaCommand command;

  @BeforeClass
  public static void buildUserSchema() throws Exception {
    schema = ReflectData.get().getSchema(User.class);
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new ObjectSchemaCommand(console);
    command.setConf(new Configuration());
  }

  @Test
  public void testSchemaStdout() throws Exception {
    command.classNames = Lists.newArrayList("org.kitesdk.cli.example.User");
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesSchema(schema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testSchemaToFile() throws Exception {
    command.classNames = Lists.newArrayList("org.kitesdk.cli.example.User");
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
  public void testMultipleSClassesFail() throws Exception {
    command.classNames = Lists.newArrayList(
        "org.kitesdk.cli.example.User", "org.kitesdk.data.Format");
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
    command.classNames = Lists.newArrayList("org.kitesdk.cli.example.User");
    command.minimize = true;
    int rc = command.run();
    Assert.assertEquals("Should return success code", 0, rc);
    verify(console).info(argThat(TestUtil.matchesMinimizedSchema(schema)));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingSamplePath() {
    TestHelpers.assertThrows("Should complain when no class name is given",
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
