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
import java.io.IOException;
import java.net.URI;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.MockRepositories;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.DatasetRepository;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.kitesdk.cli.commands.DeleteCommand.viewMatches;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDeleteCommand {

  private DatasetRepository repo;
  private DeleteCommand command;
  private Logger console;

  @Before
  public void setUp() {
    this.repo = MockRepositories.newMockRepository();
    this.console = mock(Logger.class);
    this.command = new DeleteCommand(console);
    this.command.repoURI = repo.getUri().toString();
    verify(repo).getUri(); // verify the above call
  }

  @Test
  public void testBasicUse() throws Exception {
    command.targets = Lists.newArrayList("users");
    command.run();
    verify(repo).delete("default", "users");
    verify(console).debug(contains("Deleted"), eq("users"));
  }

  @Test
  public void testMultipleDatasets() throws Exception {
    command.targets = Lists.newArrayList("users", "moreusers");
    command.run();
    verify(repo).delete("default", "users");
    verify(console).debug(contains("Deleted"), eq("users"));
    verify(repo).delete("default", "moreusers");
    verify(console).debug(contains("Deleted"), eq("moreusers"));
  }

  @Test
  public void testDatasetUri() throws Exception {
    String datasetUri = new URIBuilder(repo.getUri(), "ns", "test")
        .build()
        .toString();
    Assert.assertTrue("Should be a dataset URI",
        datasetUri.startsWith("dataset:"));
    command.targets = Lists.newArrayList(datasetUri);
    command.run();
    verify(repo).delete("ns", "test");
    verify(console).debug(contains("Deleted"), eq(datasetUri));
  }

  @Test
  public void testViewUri() throws Exception {
    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(SchemaBuilder.record("Test").fields().requiredInt("prop").endRecord())
        .build();

    URI actualViewUri = new URIBuilder(repo.getUri(), "ns", "test")
        .with("prop", "34")
        .build();
    String viewUri = actualViewUri.toString();

    Assert.assertTrue("Should be a view URI",
        viewUri.startsWith("view:"));

    AbstractDataset ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(desc);
    AbstractRefinableView view = mock(AbstractRefinableView.class);
    when(ds.filter(any(Constraints.class))).thenReturn(view);
    when(view.getUri()).thenReturn(actualViewUri);

    command.targets = Lists.newArrayList(viewUri);
    command.run();

    verify(repo).load("ns", "test", GenericRecord.class);
    verify(view).deleteAll();
    verify(console).debug(contains("Deleted"), eq(viewUri));
  }

  @Test
  public void testViewUriWithTypo() throws Exception {
    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(SchemaBuilder.record("Test").fields()
            .requiredLong("ts")
            .endRecord())
        .partitionStrategy(new PartitionStrategy.Builder()
            .year("ts")
            .month("ts")
            .day("ts")
            .build())
        .build();

    String viewUri = new URIBuilder(repo.getUri(), "ns", "test")
        .with("year", "2014")
        .with("month", "3")
        .with("dy", "14")
        .build()
        .toString();
    URI actualViewUri = new URIBuilder(repo.getUri(), "ns", "test")
        .with("month", "3")
        .with("year", "2014")
        .build();

    Assert.assertTrue("Should be a view URI",
        viewUri.startsWith("view:"));

    AbstractDataset ds = mock(AbstractDataset.class);
    when(repo.load("ns", "test", GenericRecord.class)).thenReturn(ds);
    when(ds.getDescriptor()).thenReturn(desc);
    AbstractRefinableView view = mock(AbstractRefinableView.class);
    when(ds.filter(any(Constraints.class))).thenReturn(view);
    when(view.getUri()).thenReturn(actualViewUri);

    command.targets = Lists.newArrayList(viewUri);
    TestHelpers.assertThrows("Should reject a view with missing attribute",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            try {
              command.run();
            } catch (IOException e) {
              throw new RuntimeException("Caught IOException", e);
            }
          }
        });
  }

  @Test
  public void testViewMatches() {
    Assert.assertTrue("Identical URI passes", viewMatches(
        URI.create("view:file:/path/to/dataset?year=2014&month=3&day=14"),
        "view:file:/path/to/dataset?year=2014&month=3&day=14"));
    Assert.assertTrue("Extra settings pass", viewMatches(
        URI.create("view:file:/path/to/dataset?year=2014&month=3&day=14&a=b"),
        "view:file:/path/to/dataset?year=2014&month=3&day=14"));

    Assert.assertFalse("Missing year fails", viewMatches(
        URI.create("view:file:/path/to/dataset?month=3&day=14"),
        "view:file:/path/to/dataset?year=2014&month=3&day=14"));
    Assert.assertFalse("Missing month fails", viewMatches(
        URI.create("view:file:/path/to/dataset?year=2014&day=14"),
        "view:file:/path/to/dataset?year=2014&month=3&day=14"));
    Assert.assertFalse("Missing day fails", viewMatches(
        URI.create("view:file:/path/to/dataset?year=2014&month=3"),
        "view:file:/path/to/dataset?year=2014&month=3&day=14"));
  }
}
