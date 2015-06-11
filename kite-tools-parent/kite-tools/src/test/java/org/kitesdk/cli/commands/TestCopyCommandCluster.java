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
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCopyCommandCluster extends MiniDFSTest {

  protected static final String source = "users_source";
  protected static final String dest = "users_dest";
  protected static final String dest_partitioned = "users_dest_partitioned";
  protected static final String avsc = "target/user.avsc";
  protected static String repoUri;

  @BeforeClass
  public static void createSourceDataset() throws Exception {
    repoUri = "hdfs://" + getDFS().getUri().getAuthority() + "/tmp/data";
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");

    String csv = "/tmp/users.csv";
    BufferedWriter writer = Files.newWriter(
        new File(csv), CSVSchemaCommand.SCHEMA_CHARSET);
    writer.append("id,username,email\n");
    writer.append("1,test,test@example.com\n");
    writer.append("2,user,user@example.com\n");
    writer.append("3,user3,user3@example.com\n");
    writer.append("4,user4,user4@example.com\n");
    writer.append("5,user5,user5@example.com\n");
    writer.append("6,user6,user6@example.com\n");
    writer.close();

    TestUtil.run("-v", "csv-schema", csv, "-o", avsc, "--class", "User",
      "--require", "id");
    TestUtil.run("create", source, "-s", avsc,
        "-r", repoUri, "-d", "target/data");
    TestUtil.run("csv-import", csv, source, "-r", repoUri, "-d", "target/data");
  }

  @AfterClass
  public static void deleteSourceDataset() throws Exception {
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");
  }

  protected Logger console;
  protected CopyCommand command;

  @Before
  public void createDestination() throws Exception {
    TestUtil.run("delete", dest, "-r", repoUri, "-d", "target/data");
    TestUtil.run("create", dest, "-s", avsc, "-r", repoUri, "-d", "target/data");
    this.console = mock(Logger.class);
    this.command = new CopyCommand(console);
    command.setConf(new Configuration());
  }

  @After
  public void deleteDestination() throws Exception {
    TestUtil.run("delete", dest, "-r", repoUri, "-d", "target/data");
    TestUtil.run("delete", dest_partitioned, "-r", repoUri, "-d", "target/data");
  }

  @Test
  public void testBasicCopy() throws Exception {
    command.repoURI = repoUri;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    int size = Iterators.size(repo.load("default", dest).newReader());
    Assert.assertEquals("Should contain copied records", 6, size);

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCopyWithoutCompaction() throws Exception {
    testCopyWithoutCompaction(1);
  }

  public void testCopyWithoutCompaction(int expectedFiles) throws Exception {
    command.repoURI = repoUri;
    command.noCompaction = true;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", dest);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", 6, size);

    Path[] paths = Iterators.toArray(ds.pathIterator(), Path.class);
    Assert.assertEquals("Should produce " + expectedFiles + " files: " + Arrays.toString(paths),
        expectedFiles, Iterators.size(ds.pathIterator()));

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCopyWithNumWriters() throws Exception {
    testCopyWithNumWriters(3);
  }

  @SuppressWarnings("unchecked")
  public void testCopyWithNumWriters(int expectedFiles) throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 3));

    command.repoURI = repoUri;
    command.numWriters = 3;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", dest);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", 6, size);

    Assert.assertEquals("Should produce " + expectedFiles + " files",
        expectedFiles, Iterators.size(ds.pathIterator()));

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testCopyWithNumPartitionWriters() throws Exception {
    // with no partitioning, the number of files per partition is ignored
    testCopyWithNumPartitionWriters(3, 4, 3);
  }

  @SuppressWarnings("unchecked")
  public void testCopyWithNumPartitionWriters(int numWriters,
                                              int filesPerPartition,
                                              int expectedFiles)
      throws IOException {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), numWriters));

    command.repoURI = repoUri;
    command.numWriters = numWriters;
    command.filesPerPartition = filesPerPartition;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("default", dest);
    int size = Iterators.size(ds.newReader());
    Assert.assertEquals("Should contain copied records", 6, size);

    Assert.assertEquals("Should produce " + expectedFiles + " files",
        expectedFiles, Iterators.size(ds.pathIterator()));

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
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
