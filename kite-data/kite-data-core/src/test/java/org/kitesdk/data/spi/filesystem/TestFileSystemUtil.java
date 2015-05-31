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

package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.DescriptorUtil;

import static org.kitesdk.data.CompressionType.Uncompressed;

public class TestFileSystemUtil {
  private static final Schema USER_SCHEMA = SchemaBuilder.record("User").fields()
      .requiredLong("id")
      .requiredString("username")
      .endRecord();

  private static final Schema EVENT_SCHEMA = SchemaBuilder.record("Event").fields()
      .requiredLong("timestamp")
      .requiredString("level")
      .requiredString("message")
      .endRecord();

  private static final Record USER = new Record(USER_SCHEMA);
  private static final Record EVENT = new Record(EVENT_SCHEMA);

  @BeforeClass
  public static void initRecords() {
    USER.put("id", 1L);
    USER.put("username", "test");
    EVENT.put("timestamp", System.currentTimeMillis());
    EVENT.put("level", "DEBUG");
    EVENT.put("message", "Useless information!");
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testEmptyDataset() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e/dataset_name");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();
    URI datasetUri = URI.create("dataset:file:" + folder.getAbsolutePath());
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .build();

    Datasets.create(datasetUri, descriptor);

    Collection<DatasetDescriptor> expected = Lists.newArrayList();
    Assert.assertEquals("Should succeed and find no datasets",
        expected, FileSystemUtil.findPotentialDatasets(fs, root));
  }

  @Test
  public void testUnpartitionedDataset() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e/dataset_name");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();
    URI datasetUri = URI.create("dataset:file:" + folder.getAbsolutePath());
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .build();

    Dataset<GenericRecord> dataset = Datasets.create(datasetUri, descriptor);

    // write two so that the descriptor uses the directory rather than a file
    writeUserToDataset(dataset);
    writeUserToDataset(dataset);

    DatasetDescriptor expected = dataset.getDescriptor();
    DatasetDescriptor actual = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertEquals("Should succeed and find an equivalent descriptor",
        expected, actual);
  }

  @Test
  public void testPartitionedDataset() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e/dataset_name");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();
    URI datasetUri = URI.create("dataset:file:" + folder.getAbsolutePath());
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("id", 4)
            .build())
        .build();

    Dataset<GenericRecord> dataset = Datasets.create(datasetUri, descriptor);

    // write two so that the descriptor uses the directory rather than a file
    writeUserToDataset(dataset);
    writeUserToDataset(dataset);

    Path datasetPath = new Path(folder.toURI());
    Path partitionPath = new Path(datasetPath, "id_hash=1");

    DatasetDescriptor actual = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Location should be at the partition directory",
        partitionPath.toUri(), actual.getLocation());
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, actual.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, actual.getFormat());
    Assert.assertFalse("Should not be partitioned", actual.isPartitioned());
  }

  @Test
  public void testSingleAvroFile() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a single Avro file
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri().getPath(),
        parent(descriptor.getLocation()).getPath());
    Assert.assertTrue("Should be a .avro file",
        descriptor.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, descriptor.getFormat());
    Assert.assertFalse("Should not be partitioned", descriptor.isPartitioned());
  }

  @Test
  public void testMultipleAvroFilesInOneFolder() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files in parent
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, parent);
    createAvroUserFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, descriptor.getFormat());
    Assert.assertFalse("Should not be partitioned", descriptor.isPartitioned());
  }

  @Test
  public void testMultipleAvroFilesInSeparateFolders() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroUserFile(fs, new Path(parent, "2"));

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("part", "int")
        .build();

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, descriptor.getFormat());
    Assert.assertEquals("Should be partitioned by part=int",
        strategy, descriptor.getPartitionStrategy());
  }

  @Test
  public void testMultipleAvroFilesAtDifferentDepths() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroUserFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("part", "int")
        .build();

    Assert.assertTrue("Should flag data at mixed depth in the directory tree",
        DescriptorUtil.isEnabled("kite.filesystem.mixed-depth", descriptor));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, descriptor.getFormat());
    Assert.assertEquals("Should be partitioned by part=int",
        strategy, descriptor.getPartitionStrategy());
  }

  @Test
  public void testMultipleMergeTablesAtDifferentDepths() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroUserFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("part", "int")
        .build();

    Assert.assertTrue("Should flag data at mixed depth in the directory tree",
        DescriptorUtil.isEnabled("kite.filesystem.mixed-depth", descriptor));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, descriptor.getFormat());
    Assert.assertEquals("Should be partitioned by part=int",
        strategy, descriptor.getPartitionStrategy());
  }

  @Test
  public void testMultipleAvroFilesInSeparateFoldersWithUnknown() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders, with an unknown
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroUserFile(fs, new Path(parent, "part=2"));
    createUnknownFile(fs, new Path(parent, "part=3"));

    Collection<DatasetDescriptor> descriptors = FileSystemUtil
        .findPotentialDatasets(fs, root);

    Assert.assertEquals("Should have 2 descriptors", 2, descriptors.size());
    DatasetDescriptor users1;
    DatasetDescriptor users2;
    DatasetDescriptor first = Iterables.getFirst(descriptors, null);
    if (first.getLocation().toString().contains("part=1")) {
      users1 = first;
      users2 = Iterables.getLast(descriptors, null);
    } else {
      users2 = first;
      users1 = Iterables.getLast(descriptors, null);
    }

    // the descriptors may be out of order, so check and swap
    if (users1.getLocation().toString().contains("part=2")) {
      users2 = Iterables.getFirst(descriptors, null);
      users1 = Iterables.getLast(descriptors, null);
    }

    Assert.assertFalse("Should not flag at mixed depth",
        users1.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=1").toUri(),
        parent(users1.getLocation()));
    Assert.assertTrue("Should be a .avro file",
        users1.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, users1.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, users1.getFormat());
    Assert.assertFalse("Should not be partitioned",
        users1.isPartitioned());

    Assert.assertFalse("Should not flag at mixed depth",
        users2.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=2").toUri(),
        parent(users2.getLocation()));
    Assert.assertTrue("Should be a .avro file",
        users2.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, users2.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, users2.getFormat());
    Assert.assertFalse("Should not be partitioned",
        users2.isPartitioned());
  }

  @Test
  public void testSingleParquetFile() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a single Avro file
    Path parent = new Path(folder.toURI());
    createParquetEventFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri().getPath(),
        parent(descriptor.getLocation()).getPath());
    Assert.assertTrue("Should be a .parquet file",
        descriptor.getLocation().toString().endsWith(".parquet"));
    Assert.assertEquals("Should use event schema",
        EVENT_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Parquet format",
        Formats.PARQUET, descriptor.getFormat());
    Assert.assertFalse("Should not be partitioned", descriptor.isPartitioned());
  }

  @Test
  public void testMultipleParquetFilesInOneFolder() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a single Avro file
    Path parent = new Path(folder.toURI());
    createParquetEventFile(fs, parent);
    createParquetEventFile(fs, parent);

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use event schema",
        EVENT_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Parquet format",
        Formats.PARQUET, descriptor.getFormat());
    Assert.assertFalse("Should not be partitioned", descriptor.isPartitioned());
  }

  @Test
  public void testMultipleParquetFilesInSeparateFolders() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders
    Path parent = new Path(folder.toURI());
    createParquetEventFile(fs, new Path(parent, "part"));
    createParquetEventFile(fs, new Path(parent, "2"));

    DatasetDescriptor descriptor = Iterables.getOnlyElement(
        FileSystemUtil.findPotentialDatasets(fs, root));

    PartitionStrategy strategy = new PartitionStrategy.Builder()
        .provided("partition_1", "string")
        .build();

    Assert.assertFalse("Should not flag at mixed depth",
        descriptor.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(), descriptor.getLocation());
    Assert.assertEquals("Should use user schema",
        EVENT_SCHEMA, descriptor.getSchema());
    Assert.assertEquals("Should have Parquet format",
        Formats.PARQUET, descriptor.getFormat());
    Assert.assertEquals("Should be partitioned by part=int",
        strategy, descriptor.getPartitionStrategy());
  }

  @Test
  public void testIncompatibleSchemaFilesInSeparateFolders() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Avro files under separate folders, with different schemas
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, new Path(parent, "part=1"));
    createAvroEventFile(fs, new Path(parent, "part=2"));

    Collection<DatasetDescriptor> descriptors = FileSystemUtil
        .findPotentialDatasets(fs, root);

    Assert.assertEquals("Should have 2 descriptors", 2, descriptors.size());
    DatasetDescriptor users;
    DatasetDescriptor events;
    DatasetDescriptor first = Iterables.getFirst(descriptors, null);
    if (first.getLocation().toString().contains("part=1")) {
      users = first;
      events = Iterables.getLast(descriptors, null);
    } else {
      events = first;
      users = Iterables.getLast(descriptors, null);
    }

    Assert.assertFalse("Should not flag at mixed depth",
        users.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=1").toUri(),
        parent(users.getLocation()));
    Assert.assertTrue("Should be a .avro file",
        users.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, users.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, users.getFormat());
    Assert.assertFalse("Should not be partitioned",
        users.isPartitioned());

    Assert.assertFalse("Should not flag at mixed depth",
        events.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=2").toUri(),
        parent(events.getLocation()));
    Assert.assertTrue("Should be a .avro file",
        events.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use event schema",
        EVENT_SCHEMA, events.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, events.getFormat());
    Assert.assertFalse("Should not be partitioned",
        events.isPartitioned());
  }

  @Test
  public void testIncompatibleSchemaParquetFilesInSeparateFolders() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a two Parquet files under separate folders, with different schemas
    Path parent = new Path(folder.toURI());
    createParquetUserFile(fs, new Path(parent, "part=1"));
    createParquetEventFile(fs, new Path(parent, "part=2"));

    Collection<DatasetDescriptor> descriptors = FileSystemUtil
        .findPotentialDatasets(fs, root);

    Assert.assertEquals("Should have 2 descriptors", 2, descriptors.size());
    DatasetDescriptor users;
    DatasetDescriptor events;
    DatasetDescriptor first = Iterables.getFirst(descriptors, null);
    if (first.getLocation().toString().contains("part=1")) {
      users = first;
      events = Iterables.getLast(descriptors, null);
    } else {
      events = first;
      users = Iterables.getLast(descriptors, null);
    }

    Assert.assertFalse("Should not flag at mixed depth",
        users.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=1").toUri(),
        parent(users.getLocation()));
    Assert.assertTrue("Should be a .parquet file",
        users.getLocation().toString().endsWith(".parquet"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, users.getSchema());
    Assert.assertEquals("Should have Parquet format",
        Formats.PARQUET, users.getFormat());
    Assert.assertFalse("Should not be partitioned",
        users.isPartitioned());

    Assert.assertFalse("Should not flag at mixed depth",
        events.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        new Path(parent, "part=2").toUri(),
        parent(events.getLocation()));
    Assert.assertTrue("Should be a .parquet file",
        events.getLocation().toString().endsWith(".parquet"));
    Assert.assertEquals("Should use event schema",
        EVENT_SCHEMA, events.getSchema());
    Assert.assertEquals("Should have Parquet format",
        Formats.PARQUET, events.getFormat());
    Assert.assertFalse("Should not be partitioned",
        events.isPartitioned());
  }

  @Test
  public void testIncompatibleFormatFilesInSameFolder() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create Avro and Parquet files under separate folders, with the same schema
    Path parent = new Path(folder.toURI());
    createAvroUserFile(fs, parent);
    createParquetUserFile(fs, parent);

    Collection<DatasetDescriptor> descriptors = FileSystemUtil
        .findPotentialDatasets(fs, root);

    Assert.assertEquals("Should have 2 descriptors", 2, descriptors.size());
    DatasetDescriptor avro;
    DatasetDescriptor parquet;
    DatasetDescriptor first = Iterables.getFirst(descriptors, null);
    if (first.getFormat() == Formats.AVRO) {
      avro = first;
      parquet = Iterables.getLast(descriptors, null);
    } else {
      parquet = first;
      avro = Iterables.getLast(descriptors, null);
    }

    Assert.assertFalse("Should not flag at mixed depth",
        avro.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(),
        parent(avro.getLocation()));
    Assert.assertTrue("Should be a .avro file",
        avro.getLocation().toString().endsWith(".avro"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, avro.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.AVRO, avro.getFormat());
    Assert.assertFalse("Should not be partitioned",
        avro.isPartitioned());

    Assert.assertFalse("Should not flag at mixed depth",
        parquet.hasProperty("kite.filesystem.mixed-depth"));
    Assert.assertEquals("Should be directly under parent",
        parent.toUri(),
        parent(parquet.getLocation()));
    Assert.assertTrue("Should be a .parquet file",
        parquet.getLocation().toString().endsWith(".parquet"));
    Assert.assertEquals("Should use user schema",
        USER_SCHEMA, parquet.getSchema());
    Assert.assertEquals("Should have Avro format",
        Formats.PARQUET, parquet.getFormat());
    Assert.assertFalse("Should not be partitioned",
        parquet.isPartitioned());
  }

  @Test
  public void testSingleUnknownFile() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a single Avro file
    Path parent = new Path(folder.toURI());
    createUnknownFile(fs, parent);

    Collection<DatasetDescriptor> expected = Lists.newArrayList();
    Assert.assertEquals("Should succeed and find no datasets",
        expected, FileSystemUtil.findPotentialDatasets(fs, root));
  }

  @Test
  public void testMultipleUnknownFiles() throws Exception {
    File folder = temp.newFolder("a/b/c/d/e");
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();

    // create a single Avro file
    Path parent = new Path(folder.toURI());
    createUnknownFile(fs, parent);
    createUnknownFile(fs, parent);

    Collection<DatasetDescriptor> expected = Lists.newArrayList();
    Assert.assertEquals("Should succeed and find no datasets",
        expected, FileSystemUtil.findPotentialDatasets(fs, root));
  }

  @Test
  public void testEmptyDirectory() throws IOException {
    Path root = new Path(temp.getRoot().toURI());
    FileSystem fs = LocalFileSystem.getInstance();
    Collection<DatasetDescriptor> expected = Lists.newArrayList();
    Assert.assertEquals("Should succeed and find no datasets",
        expected, FileSystemUtil.findPotentialDatasets(fs, root));
  }

  @Test
  public void testMissingDirectory() throws IOException {
    final Path root = new Path(new Path(temp.getRoot().toURI()), "not_there");
    final FileSystem fs = LocalFileSystem.getInstance();

    TestHelpers.assertThrows("Should propagate missing file IOException",
        FileNotFoundException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            FileSystemUtil.findPotentialDatasets(fs, root);
            return null;
          }
        });
  }

  private URI parent(URI file) {
    return new Path(file).getParent().toUri();
  }

  public void writeUserToDataset(Dataset<GenericRecord> dataset) {
    DatasetWriter<GenericRecord> writer = null;
    try {
      writer = dataset.newWriter();
      writer.write(USER);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public void createAvroUserFile(FileSystem fs, Path parent) throws IOException {
    Path file = new Path(parent, UUID.randomUUID().toString() + ".avro");
    AvroAppender<Record> appender = new AvroAppender<Record>(
        fs, file, USER_SCHEMA, Uncompressed);
    appender.open();
    appender.append(USER);
    appender.close();
  }

  public void createAvroEventFile(FileSystem fs, Path parent) throws IOException {
    Path file = new Path(parent, UUID.randomUUID().toString() + ".avro");
    AvroAppender<Record> appender = new AvroAppender<Record>(
        fs, file, EVENT_SCHEMA, Uncompressed);
    appender.open();
    appender.append(EVENT);
    appender.close();
  }

  public void createParquetUserFile(FileSystem fs, Path parent) throws IOException {
    Path file = new Path(parent, UUID.randomUUID().toString() + ".parquet");
    ParquetAppender<Record> appender = new ParquetAppender<Record>(
        fs, file, USER_SCHEMA, new Configuration(), Uncompressed);
    appender.open();
    appender.append(USER);
    appender.close();
  }

  public void createParquetEventFile(FileSystem fs, Path parent) throws IOException {
    Path file = new Path(parent, UUID.randomUUID().toString() + ".parquet");
    ParquetAppender<Record> appender = new ParquetAppender<Record>(
        fs, file, EVENT_SCHEMA, new Configuration(), Uncompressed);
    appender.open();
    appender.append(EVENT);
    appender.close();
  }

  public void createUnknownFile(FileSystem fs, Path parent) throws IOException {
    Path file = new Path(parent, UUID.randomUUID().toString() + ".unknown");
    fs.create(file);
  }
}
