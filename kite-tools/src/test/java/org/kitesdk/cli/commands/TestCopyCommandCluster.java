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
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
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
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.URIBuilder;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;
import org.kitesdk.data.spi.filesystem.FileSystemDataset;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestCopyCommandCluster extends MiniDFSTest {

  private static final String source = "users_source";
  private static final String dest = "users_dest";
  private static final String avsc = "target/user.avsc";
  private static final Pattern UPPER_CASE = Pattern.compile("^[A-Z]+\\d*$");
  private static String repoUri;

  @BeforeClass
  public static void createSourceDataset() throws Exception {
    repoUri = "hdfs://" + getDFS().getUri().getAuthority() + "/tmp/data";
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");

    String csv = "target/users.csv";
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

    TestUtil.run("-v", "csv-schema", csv, "-o", avsc, "--class", "User");
    TestUtil.run("create", source, "-s", avsc,
        "-r", repoUri, "-d", "target/data");
    TestUtil.run("csv-import", csv, source, "-r", repoUri, "-d", "target/data");
  }

  @AfterClass
  public static void deleteSourceDataset() throws Exception {
    TestUtil.run("delete", source, "-r", repoUri, "-d", "target/data");
  }

  private Logger console;
  private CopyCommand command;

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
  }

  @Test
  public void testBasicCopy() throws Exception {
    command.repoURI = repoUri;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    int size = DatasetTestUtilities.datasetSize(repo.load(dest));
    Assert.assertEquals("Should contain copied records", 6, size);

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
    verifyNoMoreInteractions(console);
  }

  @Test
  public void testTransform() throws Exception {
    command.repoURI = repoUri;
    command.transform = "org.kitesdk.cli.example.ToUpperCase";
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    Set<GenericRecord> records = DatasetTestUtilities.materialize(
        repo.<GenericRecord>load(dest));
    Assert.assertEquals("Should contain copied records", 6, records.size());
    for (GenericRecord record : records) {
      Assert.assertTrue("Username should be upper case",
          UPPER_CASE.matcher(record.get("username").toString()).matches());
    }
  }

  @Test
  public void testCopyWithoutCompaction() throws Exception {
    command.repoURI = repoUri;
    command.noCompaction = true;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load(dest);
    int size = DatasetTestUtilities.datasetSize(ds);
    Assert.assertEquals("Should contain copied records", 6, size);

    Assert.assertEquals("Should produce 1 files",
        1, Iterators.size(ds.pathIterator()));

    verify(console).info("Added {} records to \"{}\"", 6l, dest);
    verifyNoMoreInteractions(console);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCopyWithNumWriters() throws Exception {
    Assume.assumeTrue(setLocalReducerMax(getConfiguration(), 3));

    command.repoURI = repoUri;
    command.numWriters = 3;
    command.datasets = Lists.newArrayList(source, dest);

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load(dest);
    int size = DatasetTestUtilities.datasetSize(ds);
    Assert.assertEquals("Should contain copied records", 6, size);

    Assert.assertEquals("Should produce 3 files",
        3, Iterators.size(ds.pathIterator()));

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

  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionedCopyWithNumWriters() throws Exception {
    command.repoURI = repoUri;
    command.numWriters = 3;
    command.datasets = Lists.newArrayList(source, "dest_partitioned");
    URI dsUri = new URIBuilder("repo:" + repoUri, "dest_partitioned").build();
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

    int rc = command.run();
    Assert.assertEquals("Should return success", 0, rc);

    DatasetRepository repo = DatasetRepositories.repositoryFor("repo:" + repoUri);
    FileSystemDataset<GenericData.Record> ds =
        (FileSystemDataset<GenericData.Record>) repo.<GenericData.Record>
            load("dest_partitioned");
    int size = DatasetTestUtilities.datasetSize(ds);
    Assert.assertEquals("Should contain copied records", 6, size);

    Assert.assertEquals("Should produce 2 partitions",
        2, Iterators.size(ds.pathIterator()));

    verify(console).info("Added {} records to \"{}\"", 6l, "dest_partitioned");
    verifyNoMoreInteractions(console);
  }

}
