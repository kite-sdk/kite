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

import com.beust.jcommander.internal.Lists;
import com.google.common.io.Files;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.Schemas;
import org.slf4j.Logger;

import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestCreateDatasetWithExistingData {

  private static final Path existingDataPath =
      new Path("target/data/users_parquet");
  private static final String existingDataURI =
      "dataset:file:target/data/users_parquet";
  private static final Path existingPartitionedPath =
      new Path("target/data/users_partitioned");
  private static final Path existingPartitionedPathWithPartition =
      new Path("target/data/users_partitioned/version=1");
  private static final String existingPartitionedURI =
      "dataset:file:target/data/users_partitioned";
  private static final String sourceDatasetURI =
      "dataset:file:target/data/users";
  private static Schema USER_SCHEMA;
  private CreateDatasetCommand command = null;
  private Logger console;

  @BeforeClass
  public static void createDatasetFromCSV() throws Exception {
    String sample = "target/users.csv";
    String avsc = "target/user.avsc";
    BufferedWriter writer = Files.newWriter(
        new File(sample), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.append("2,user,user@example.com\n");
    writer.close();

    TestUtil.run("delete", "dataset:file:target/data/users");
    TestUtil.run("-v", "csv-schema", sample, "-o", avsc, "--class", "User");
    TestUtil.run("-v", "create", "dataset:file:target/data/users", "-s", avsc,
        "-f", "parquet");
    TestUtil.run("-v", "csv-import", sample, "dataset:file:target/data/users");

    USER_SCHEMA = Schemas.fromAvsc(new File(avsc));

    FileSystem fs = LocalFileSystem.getInstance();
    FileStatus[] stats = fs.listStatus(new Path("target/data/users"));
    Path parquetFile = null;
    for (FileStatus stat : stats) {
      if (stat.getPath().toString().endsWith(".parquet")) {
        parquetFile = stat.getPath();
        break;
      }
    }

    // make a directory with the Parquet file
    fs.mkdirs(existingDataPath);
    fs.copyFromLocalFile(parquetFile, existingDataPath);
    fs.mkdirs(existingPartitionedPathWithPartition);
    fs.copyFromLocalFile(parquetFile, existingPartitionedPathWithPartition);
  }

  @AfterClass
  public static void removeData() throws Exception {
    TestUtil.run("delete", "dataset:file:target/data/users");
    FileSystem fs = LocalFileSystem.getInstance();
    fs.delete(existingDataPath, true);
    fs.delete(existingPartitionedPath, true);
  }

  @Before
  public void setup() throws Exception {
    this.console = mock(Logger.class);
    this.command = new CreateDatasetCommand(console);
    this.command.setConf(new Configuration());
  }

  @After
  public void removeMetadata() throws Exception {
    FileSystem fs = LocalFileSystem.getInstance();
    fs.delete(new Path(existingDataPath, ".metadata"), true);
    fs.delete(new Path(existingPartitionedPath, ".metadata"), true);
  }

  @Test
  public void testCreateFromExisting() throws Exception {
    command.datasets = Lists.newArrayList(existingDataURI);
    command.run();

    verify(console).debug(contains("Created"), eq(existingDataURI));

    // load the new dataset and verify it
    Dataset<GenericRecord> users = Datasets.load(existingDataURI);
    Assert.assertEquals("Schema should match",
        USER_SCHEMA, users.getDescriptor().getSchema());
    Assert.assertFalse("Should not be partitioned",
        users.getDescriptor().isPartitioned());
    Assert.assertEquals("Should be Parquet",
        Formats.PARQUET, users.getDescriptor().getFormat());
  }

  @Test
  public void testCreateFromExistingWithLocation() throws Exception {
    command.datasets = Lists.newArrayList(existingDataURI);
    command.location = existingPartitionedPathWithPartition.toString();
    command.run();

    verify(console).debug(contains("Created"), eq(existingDataURI));

    // load the new dataset and verify it
    Dataset<GenericRecord> users = Datasets.load(existingDataURI);
    Assert.assertEquals("Schema should match",
        USER_SCHEMA, users.getDescriptor().getSchema());
    Assert.assertFalse("Should not be partitioned",
        users.getDescriptor().isPartitioned());
    Assert.assertEquals("Should be Parquet",
        Formats.PARQUET, users.getDescriptor().getFormat());
    Assert.assertTrue("Location should point to the partitioned data",
        String.valueOf(users.getDescriptor().getLocation())
            .endsWith(existingPartitionedPathWithPartition.toString()));
  }

  @Test
  public void testFailCreateFormatMismatch() throws Exception {
    command.datasets = Lists.newArrayList(existingDataURI);
    command.format = "avro";

    TestHelpers.assertThrows(
        "Should reject Avro format when Parquet data exists",
        ValidationException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testFailCreateSchemaCannotReadExisting() throws Exception {
    Schema requiresId = SchemaBuilder.record("User").fields()
        .requiredLong("id")
        .optionalString("username")
        .optionalString("email")
        .endRecord();

    File avsc = new File("target/user_requires_id.avsc");
    FileWriter writer = new FileWriter(avsc);
    writer.append(requiresId.toString());
    writer.close();

    command.datasets = Lists.newArrayList(existingDataURI);
    command.avroSchemaFile = avsc.toString();

    TestHelpers.assertThrows(
        "Should reject incompatible schema",
        ValidationException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            command.run();
            return null;
          }
        });

    Assert.assertTrue(avsc.delete());
  }

  @Test
  public void testFailCreateIfDatasetExists() throws Exception {
    command.datasets = Lists.newArrayList(sourceDatasetURI);

    TestHelpers.assertThrows(
        "Should fail because the dataset already exists",
        DatasetExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            command.run();
            return null;
          }
        });
  }

  @Test
  public void testCreateFromExistingPartitioned() throws Exception {
    command.datasets = Lists.newArrayList(existingPartitionedURI);
    command.run();

    verify(console).debug(contains("Created"), eq(existingPartitionedURI));

    PartitionStrategy providedVersionStrategy = new PartitionStrategy.Builder()
        .provided("version", "int")
        .build();

    // load the new dataset and verify it
    Dataset<GenericRecord> users = Datasets.load(existingPartitionedURI);
    Assert.assertEquals("Schema should match",
        USER_SCHEMA, users.getDescriptor().getSchema());
    Assert.assertEquals("Should be partitioned with a provided partitioner",
        providedVersionStrategy, users.getDescriptor().getPartitionStrategy());
    Assert.assertEquals("Should be Parquet",
        Formats.PARQUET, users.getDescriptor().getFormat());
  }

  @Test
  public void testFailIncompatiblePartitionStrategy() throws Exception {
    // create a partition strategy using the new schema field
    PartitionStrategy versionStrategy = new PartitionStrategy.Builder()
        .year("id")
        .build();

    File strategy = new File("target/strategy.json");
    FileWriter writer = new FileWriter(strategy);
    writer.append(versionStrategy.toString());
    writer.close();

    command.datasets = Lists.newArrayList(existingPartitionedURI);
    command.partitionStrategyFile = strategy.toString();

    TestHelpers.assertThrows(
        "Should reject incompatible partition strategy",
        ValidationException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            command.run();
            return null;
          }
        });

    Assert.assertTrue(strategy.delete());
  }

  @Test
  public void testCreateFromExistingWithPartitionAndSchemaUpdate() throws Exception {
    // write an updated schema
    Schema versionAdded = SchemaBuilder.record("User").fields()
        .optionalLong("id")
        .optionalString("username")
        .optionalString("email")
        .name("v").type().longType().longDefault(1L)
        .endRecord();

    File avsc = new File("target/user_version_added.avsc");
    FileWriter writer = new FileWriter(avsc);
    writer.append(versionAdded.toString());
    writer.close();

    // create a partition strategy using the new schema field
    PartitionStrategy versionStrategy = new PartitionStrategy.Builder()
        .identity("v", "version")
        .build();

    File strategy = new File("target/strategy.json");
    writer = new FileWriter(strategy);
    writer.append(versionStrategy.toString());
    writer.close();

    command.datasets = Lists.newArrayList(existingPartitionedURI);
    command.avroSchemaFile = avsc.toString();
    command.partitionStrategyFile = strategy.toString();
    command.run();

    verify(console).debug(contains("Created"), eq(existingPartitionedURI));

    // load the new dataset and verify it
    Dataset<GenericRecord> users = Datasets.load(existingPartitionedURI);
    Assert.assertEquals("Schema should match",
        versionAdded, users.getDescriptor().getSchema());
    Assert.assertEquals("Should be partitioned with a provided partitioner",
        versionStrategy, users.getDescriptor().getPartitionStrategy());
    Assert.assertEquals("Should be Parquet",
        Formats.PARQUET, users.getDescriptor().getFormat());

    Assert.assertTrue(avsc.delete());
    Assert.assertTrue(strategy.delete());
  }
}
