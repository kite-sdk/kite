/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.cli.commands;

import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.MockRepositories;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestCreateDatasetCommand {

  private DatasetRepository repo;
  private CreateDatasetCommand command = null;
  private Logger console;

  @Before
  public void setUp() {
    this.repo = MockRepositories.newMockRepository();
    this.console = mock(Logger.class);
    this.command = new CreateDatasetCommand(console);
    this.command.setConf(new Configuration());
    this.command.repoURI = repo.getUri().toString();
    verify(repo).getUri(); // verify the above call
  }

  @Test
  public void testBasicUse() throws Exception {
    command.avroSchemaFile = "resource:test-schemas/user.avsc";
    command.datasets = Lists.newArrayList("users");
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .build();

    verify(repo).create("default", "users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testParquetFormat() throws Exception {
    command.avroSchemaFile = "resource:test-schemas/user.avsc";
    command.datasets = Lists.newArrayList("users");
    command.format = "parquet";
    command.run();

    DatasetDescriptor expectedDescriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:test-schemas/user.avsc")
        .format("parquet")
        .build();

    verify(repo).create("default", "users", expectedDescriptor);
    verify(console).debug(contains("Created"), eq("users"));
  }

  @Test
  public void testUnrecognizedFormat() throws Exception {
    command.avroSchemaFile = "resource:test-schemas/user.avsc";
    command.datasets = Lists.newArrayList("users");
    command.format = "nosuchformat";
    TestHelpers.assertThrows("Should fail on invalid format",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Void call() throws Exception {
            command.run();
            return null;
          }
        });
    verifyZeroInteractions(console);
  }

  @Test
  public void testNonExistentAvroSchemaFile() throws Exception {
    command.avroSchemaFile = "nonexistent.avsc";
    command.datasets = Lists.newArrayList("users");
    TestHelpers.assertThrows("Should fail on missing schema",
        FileNotFoundException.class, new Callable() {
          @Override
          public Void call() throws Exception {
            command.run();
            return null;
          }
        });
    verifyZeroInteractions(console);
  }

  @Test
  public void testMultipleDatasets() throws Exception {
    command.avroSchemaFile = "test-schemas/user.avsc";
    command.datasets = Lists.newArrayList("users", "moreusers");
    TestHelpers.assertThrows("Should reject multiple dataset names",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Void call() throws Exception {
            command.run();
            return null;
          }
        });
    verifyZeroInteractions(console);
  }

}
