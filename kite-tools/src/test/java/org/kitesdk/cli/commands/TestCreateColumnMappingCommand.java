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

import com.beust.jcommander.internal.Lists;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCreateColumnMappingCommand {
  private Logger console;
  private CreateColumnMappingCommand command;

  @Before
  public void setupCommand() {
    this.console = mock(Logger.class);
    this.command = new CreateColumnMappingCommand(console);
    command.setConf(new Configuration());
    command.avroSchemaFile = "resource:test-schemas/hbase-user.avsc";
    command.partitionStrategyFile = "resource:test-partitions/email-part.json";
  }

  @Test
  public void testBasic() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u", "created_at:u");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "username")
        .counter("created_at", "u", "created_at")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testQualifier() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u:n");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "n")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testVersion() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u:n", "version:version");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .version("version")
        .key("email")
        .column("username", "u", "n")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testVersionWithCounterTypes() throws Exception {
    // longs and ints are mapped
    command.partitions = Lists.newArrayList(
        "email:key", "version:version", "username:u", "created_at:u");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .version("version")
        .key("email")
        .column("username", "u", "username")
        .column("created_at", "u", "created_at")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCounter() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u", "created_at:u");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "username")
        .counter("created_at", "u", "created_at")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCounterWithQualifier() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u", "created_at:u:ts");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "username")
        .counter("created_at", "u", "ts")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testKAC() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u", "preferences:prefs");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "username")
        .keyAsColumn("preferences", "prefs")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testKACWithPrefix() throws Exception {
    command.partitions = Lists.newArrayList(
        "email:key", "username:u", "preferences:prefs:p_");
    command.run();

    ColumnMapping mapping = new ColumnMapping.Builder()
        .key("email")
        .column("username", "u", "username")
        .keyAsColumn("preferences", "prefs", "p_")
        .build();
    verify(console).info(mapping.toString(true));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingKeyPartition() throws Exception {
    // does not include an identity partition for email
    command.partitionStrategyFile = "resource:test-partitions/email-hash-part.json";
    command.partitions = Lists.newArrayList(
        "email:key"
    );
    TestHelpers.assertThrows("Should reject missing partitioner",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownKeySourceField() {
    command.partitions = Lists.newArrayList("panda:key");
    TestHelpers.assertThrows("Should reject unknown key field \"panda\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownVersionSourceField() {
    command.partitions = Lists.newArrayList("grizzly:version");
    TestHelpers.assertThrows("Should reject unknown version field \"grizzly\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownSourceField() {
    command.partitions = Lists.newArrayList("koala:u");
    TestHelpers.assertThrows("Should reject unknown field \"koala\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testMissingFamily() {
    command.partitions = Lists.newArrayList("gummi:");
    TestHelpers.assertThrows("Should reject missing family for \"gummi\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testUnknownMapping() {
    command.partitions = Lists.newArrayList("kodiak");
    TestHelpers.assertThrows("Should reject unknown mapping \"kodiak\"",
        ValidationException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
  }
}
