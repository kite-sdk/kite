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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.MockRepositories;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

public class TestUpdateDatasetCommand {

  private DatasetRepository repo;
  private Dataset<Object> ds;
  private UpdateDatasetCommand command;
  private Logger console;
  private DatasetDescriptor original;

  String schema1 = "{\"type\": \"record\", \"name\": \"Record\", " +
      "  \"fields\": [" +
      "    { \"name\": \"f0\", \"type\": \"string\" }" +
      "  ]}";
  String schema2 = "{\"type\": \"record\", \"name\": \"Record\", " +
      "  \"fields\": [" +
      "    { \"name\": \"f0\", \"type\": \"string\" }," +
      "    { \"name\": \"f1\", \"type\": \"int\" }" +
      "  ]}";

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    this.repo = MockRepositories.newMockRepository();
    this.console = mock(Logger.class);
    this.command = new UpdateDatasetCommand(console);
    this.command.repoURI = repo.getUri().toString();
    this.command.setConf(new Configuration());
    verify(repo).getUri(); // verify the above call

    this.original = new DatasetDescriptor.Builder()
        .schemaLiteral(schema1)
        .property("should.be.present", "true")
        .build();

    this.ds = mock(Dataset.class);
    when(repo.load("users")).thenReturn(ds);
    when(ds.getDataset()).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(original);
  }

  @Test
  public void testBasicUse() throws Exception {
    command.datasets = Lists.newArrayList("users");
    //
    command.run();

    verify(repo).load("users"); // need to load the current dataset
    verify(ds).getDescriptor(); // should inspect and use its descriptor
    verify(repo).update(eq("users"), argThat(TestUtil.matches(original)));
    verify(console).debug(contains("Updated"), eq("users"));
  }

  @Test
  public void testUpdateSchema() throws Exception {
    File avroSchemaFile = new File("target/schema_update.avsc");
    new FileWriter(avroSchemaFile).append(schema2).close();

    command.datasets = Lists.newArrayList("users");
    command.avroSchemaFile = avroSchemaFile.toString();
    command.run();

    DatasetDescriptor updated = new DatasetDescriptor.Builder(original)
        .schemaLiteral(schema2)
        .build();

    verify(repo).load("users"); // need to load the current dataset
    verify(ds).getDescriptor(); // should inspect and use its descriptor
    verify(repo).update(eq("users"), argThat(TestUtil.matches(updated)));
    verify(console).debug(contains("Updated"), eq("users"));
  }

  @Test
  public void testAddProperty() throws Exception {
    command.datasets = Lists.newArrayList("users");
    command.properties = Lists.newArrayList(
        "new.property=1234",
        "prop=with=equals"
    );
    command.run();

    DatasetDescriptor updated = new DatasetDescriptor.Builder(original)
        .property("new.property", "1234")
        .property("prop", "with=equals")
        .build();

    verify(repo).load("users"); // need to load the current dataset
    verify(ds).getDescriptor(); // should inspect and use its descriptor
    verify(repo).update(eq("users"), argThat(TestUtil.matches(updated)));
    verify(console).debug(contains("Updated"), eq("users"));
  }

  @Test
  public void testZeroDatasets() throws Exception {
    command.datasets = Lists.newArrayList();
    TestHelpers.assertThrows("Should require one dataset",
        IllegalArgumentException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            command.run();
            return null;
          }
        });
    verifyZeroInteractions(console);
  }

  @Test
  public void testMultipleDatasets() throws Exception {
    command.datasets = Lists.newArrayList("users", "moreusers");
    TestHelpers.assertThrows("Should reject multiple datasets",
        IllegalArgumentException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
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

}
