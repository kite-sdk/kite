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
import com.beust.jcommander.internal.Sets;
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

public class TestCSVImportCommand {

  private static String sample;
  private static String avsc;
  private static String datasetName;

  private Logger console = null;
  private CSVImportCommand command;
  private Dataset<GenericData.Record> dataset;

  private static final Set<GenericData.Record> expected = Sets.newHashSet();

  @BeforeClass
  public static void createCSVSchemaAndSample() throws Exception {
    sample = "target/users.csv";
    avsc = "target/user.avsc";
    datasetName = "users";

    BufferedWriter writer = Files.newWriter(
        new File(sample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.append("2,user,user@example.com\n");
    writer.close();

    TestUtil.run("-v", "csv-schema", sample, "-o", avsc, "--class", "User");

    GenericRecordBuilder builder = new GenericRecordBuilder(
        new Schema.Parser().parse(new File(avsc)));
    builder.set("id", 1l);
    builder.set("username", "test");
    builder.set("email", "test@example.com");
    expected.add(builder.build());
    builder.set("id", 2l);
    builder.set("username", "user");
    builder.set("email", "user@example.com");
    expected.add(builder.build());
  }

  @Before
  public void setup() throws Exception {
    TestUtil.run("-v", "create", datasetName,
        "--use-local", "-d", "target/data", "-s", avsc);
    this.dataset = Datasets.load(
        URIBuilder.build("repo:file:target/data", "default", datasetName),
        GenericData.Record.class);
    this.console = mock(Logger.class);
    this.command = new CSVImportCommand(console);
    command.setConf(new Configuration());
    // set the test repository information
    command.local = true;
    command.directory = "target/data";
  }

  @After
  public void removeData() throws Exception {
    TestUtil.run("delete", datasetName, "--use-local", "-d", "target/data");
  }

  @Test
  public void testBasicImport() throws Exception {
    command.targets = Lists.newArrayList(sample, datasetName);
    command.run();
    Assert.assertEquals("Should contain expected records",
        expected, DatasetTestUtilities.materialize(dataset));
    verify(console).trace(contains("repo:file:target/data"));
    verify(console).info("Added {} records to \"{}\"", 2l, datasetName);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testDirectoryImport() throws Exception {
    new File("target/sample").mkdir();
    BufferedWriter writer = Files.newWriter(
        new File("target/sample/one.csv"), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.close();

    writer = Files.newWriter(
        new File("target/sample/two.csv"), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("2,user,user@example.com\n");
    writer.close();

    command.targets = Lists.newArrayList("target/sample", datasetName);
    command.run();
    Assert.assertEquals("Should contain expected records",
        expected, DatasetTestUtilities.materialize(dataset));
    verify(console).trace(contains("repo:file:target/data"));
    verify(console).info("Added {} records to \"{}\"", 2l, datasetName);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingSamplePath() throws Exception {
    command.targets = Lists.newArrayList("missing.csv", datasetName);
    TestHelpers.assertThrows("Should complain about missing CSV data path",
        IllegalArgumentException.class, new Callable() {
      @Override
      public Object call() throws Exception {
        command.run();
        return null;
      }
    });
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testEmptySampleDirectory() throws Exception {
    new File("target/emptyDir").mkdir();
    command.targets = Lists.newArrayList("target/emptyDir", datasetName);
    TestHelpers.assertThrows("Should complain about no data files",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        });
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testMissingDataset() throws Exception {
    command.targets = Lists.newArrayList(sample, "notadataset");
    TestHelpers.assertThrows("Should complain about missing dataset",
        DatasetNotFoundException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        }
    );
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testIncompatibleSchemaFieldOrder() throws Exception {
    BufferedWriter writer = Files.newWriter(
        new File("target/incompatible.csv"), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("email,username,id\n");
    writer.append("test@example.com,test,1\n");
    writer.close();

    command.targets = Lists.newArrayList("target/incompatible.csv", datasetName);
    TestHelpers.assertThrows("Should complain about field order",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        }
    );
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testIncompatibleSchemaFieldType() throws Exception {
    BufferedWriter writer = Files.newWriter(
        new File("target/incompatible.csv"), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("NaN,test,test@example.com\n"); // id will be String
    writer.close();

    command.targets = Lists.newArrayList("target/incompatible.csv", datasetName);
    TestHelpers.assertThrows("Should complain about schema compatibility",
        IllegalArgumentException.class, new Callable() {
          @Override
          public Object call() throws Exception {
            command.run();
            return null;
          }
        }
    );
    verify(console).trace(contains("repo:file:target/data"));
    verifyNoMoreInteractions(console);
  }

}
