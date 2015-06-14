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
import com.google.common.collect.Iterators;
import com.google.common.io.Files;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.net.URI;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCompactCommandCluster extends MiniDFSTest {

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private static final String unpartitioned = "users_source";
  private static final String partitioned = "users_source_partitioned";
  private static final String avsc = "target/user.avsc";
  private static String repoUri;
  private int numRecords;

  @Before
  public void createDatasets() throws Exception {
    repoUri = "hdfs://" + getDFS().getUri().getAuthority() + "/tmp/data";
    TestUtil.run("delete", unpartitioned, "-r", repoUri, "-d", "target/data");

    File csvFile = temp.newFile("users.csv");
    csvFile.delete();
    String csv = csvFile.toString();
    BufferedWriter writer = Files.newWriter(
        csvFile, CSVSchemaCommand.SCHEMA_CHARSET);

    writer.append("id,username,email\n");
    numRecords = 30;
    for(int i = 0; i < numRecords; i++) {
      writer.append(i+",test"+i+",test"+i+"@example.com\n");
    }
    writer.close();

    TestUtil.run("-v", "csv-schema", csv, "-o", avsc, "--class", "User");
    TestUtil.run("create", unpartitioned, "-s", avsc,
        "-r", repoUri, "-d", "target/data");

    URI dsUri = URIBuilder.build("repo:" + repoUri, "default", partitioned);
    Datasets.<Object, Dataset<Object>>create(dsUri, new DatasetDescriptor.Builder()
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("id", 2)
            .build())
        .schema(SchemaBuilder.record("User").fields()
            .requiredLong("id")
            .optionalString("username")
            .optionalString("email")
            .endRecord())
        .build(), Object.class);


    TestUtil.run("csv-import", csv, unpartitioned, "-r", repoUri, "-d", "target/data");
    TestUtil.run("csv-import", csv, partitioned, "-r", repoUri, "-d", "target/data");
  }

  @Before
  public void createCommand(){
    this.console = mock(Logger.class);
    this.command = new CompactCommand(console);
    command.setConf(new Configuration());
  }

  @After
  public void deleteSourceDatasets() throws Exception {
    TestUtil.run("delete", unpartitioned, "-r", repoUri, "-d", "target/data");
    TestUtil.run("delete", partitioned, "-r", repoUri, "-d", "target/data");
  }

  private Logger console;
  private CompactCommand command;

  @Test
  public void testBasicUnpartitionedCompact() throws Exception {
    command.repoURI = repoUri;
    command.datasets = Lists.newArrayList(unpartitioned);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    int size = Iterators.size(repo.load("default", unpartitioned).newReader());
    Assert.assertEquals("Should contain copied records", numRecords, size);

    verify(console).info("Compacted {} records in \"{}\"", (long)numRecords, unpartitioned);
    verifyNoMoreInteractions(console);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactUnpartitionedWithNumWriters() throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 3));

    command.repoURI = repoUri;
    command.numWriters = 3;
    command.datasets = Lists.newArrayList(unpartitioned);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", unpartitioned);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", numRecords, size);

    Assert.assertEquals("Should produce 3 files",
        3, Iterators.size(ds.pathIterator()));

    verify(console).info("Compacted {} records in \"{}\"",(long) numRecords, unpartitioned);
    verifyNoMoreInteractions(console);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompactUnpartitionedWithNumPartitionWriters() throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 3));

    command.repoURI = repoUri;
    command.numWriters = 3;
    command.filesPerPartition = 4;
    command.datasets = Lists.newArrayList(unpartitioned);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", unpartitioned);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", numRecords, size);

    // ignores numPartitionWriters
    Assert.assertEquals("Should produce 3 files",
        3, Iterators.size(ds.pathIterator()));

    verify(console).info("Compacted {} records in \"{}\"", (long)numRecords, unpartitioned);
    verifyNoMoreInteractions(console);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionedCompactWithNumWriters() throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 2));
    command.repoURI = repoUri;
    command.numWriters = 2;
    command.filesPerPartition = 1;
    command.datasets = Lists.newArrayList(partitioned);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", partitioned);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", numRecords, size);

    Assert.assertEquals("Should produce 2 partitions", 2, Iterators.size(ds.getCoveringPartitions().iterator()));
    Assert.assertEquals(
        "Should produce 2 files: " + Iterators.toString(ds.pathIterator()),
        2, Iterators.size(ds.pathIterator()));

    verify(console).info("Compacted {} records in \"{}\"", (long) numRecords, partitioned);
    verifyNoMoreInteractions(console);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionedCompactWithNumWritersNumFilesPerPartition() throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 2));
    command.repoURI = repoUri;
    // if a reducer gets multiple parts of a partition, they will be combined
    // use more reducers to reduce the likelihood of that case
    command.numWriters = 10;
    command.filesPerPartition = 3;
    command.datasets = Lists.newArrayList(partitioned);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", partitioned);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", numRecords, size);

    Assert.assertEquals("Should produce 2 partitions", 2, Iterators.size(ds.getCoveringPartitions().iterator()));
    Assert.assertEquals("Should produce 6 files", 6, Iterators.size(ds.pathIterator()));

    verify(console).info("Compacted {} records in \"{}\"", (long)numRecords, partitioned);
    verifyNoMoreInteractions(console);
  }

  private boolean setLocalReducerMax(Configuration conf, int max) {
    try {
      Job job = Hadoop.Job.newInstance.invoke(new Configuration(false));
      DynMethods.StaticMethod setReducerMax = new DynMethods
          .Builder("setLocalMaxRunningReduces")
          .impl(LocalJobRunner.class,
              org.apache.hadoop.mapreduce.JobContext.class, Integer.TYPE)
          .buildStaticChecked();
      setReducerMax.invoke(job, max);
      // copy the setting into the passed configuration
      Configuration jobConf = Hadoop.JobContext.getConfiguration.invoke(job);
      for (Map.Entry<String, String> entry : jobConf) {
        conf.set(entry.getKey(), entry.getValue());
      }
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
