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
package org.kitesdk.data.crunch;

import com.google.common.io.Files;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import junit.framework.Assert;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.kitesdk.data.hcatalog.HCatalogDatasetRepository;

import static org.kitesdk.data.filesystem.DatasetTestUtilities.USER_SCHEMA;
import static org.kitesdk.data.filesystem.DatasetTestUtilities.checkTestUsers;
import static org.kitesdk.data.filesystem.DatasetTestUtilities.datasetSize;
import static org.kitesdk.data.filesystem.DatasetTestUtilities.writeTestUsers;

@RunWith(Parameterized.class)
public class TestCrunchDatasets extends MiniDFSTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    MiniDFSTest.setupFS();
    Object[][] data = new Object[][] {
        { getDFS() },
        { getFS() }
    };
    return Arrays.asList(data);
  }

  private FileSystem fileSystem;
  private DatasetRepository repo;

  public TestCrunchDatasets(FileSystem fs) {
    this.fileSystem = fs;
  }

  @Before
  public void setUp() throws Exception {
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.repo = new FileSystemDatasetRepository.Builder()
        .configuration(fileSystem.getConf())
        .rootDirectory(testDirectory).build();
  }

  @Test
  public void testGeneric() throws IOException {
    Dataset<Record> inputDataset = repo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = repo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkTestUsers(outputDataset, 10);
  }

  @Test
  public void testGenericParquet() throws IOException {
    // Parquet fails when testing with HDFS because
    // parquet.hadoop.ParquetReader calls new Configuration(), which does
    // not work with the mini-cluster (not set up through env).
    Assume.assumeTrue(fileSystem instanceof LocalFileSystem);

    Dataset<Record> inputDataset = repo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());
    Dataset<Record> outputDataset = repo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkTestUsers(outputDataset, 10);
  }

  @Test
  public void testHive() throws IOException {
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    DatasetRepository hiveRepo = new HCatalogDatasetRepository.Builder()
        .configuration(fileSystem.getConf())
        .rootDirectory(testDirectory).build();

    Dataset<Record> inputDataset = hiveRepo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = hiveRepo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkTestUsers(outputDataset, 10);

    hiveRepo.delete("in");
    hiveRepo.delete("out");
  }

  @Test
  public void testPartitionedSource() throws IOException {
    // Parquet fails when testing with HDFS because
    // parquet.hadoop.ParquetReader calls new Configuration(), which does
    // not work with the mini-cluster (not set up through env).
    Assume.assumeTrue(fileSystem instanceof LocalFileSystem);

    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = partitionStrategy.partitionKey(0);
    Dataset<Record> inputPart0 = inputDataset.getPartition(key, false);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputDataset));
  }

  @Test
  public void testPartitionedSourceAndTarget() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = partitionStrategy.partitionKey(0);
    Dataset<Record> inputPart0 = inputDataset.getPartition(key, false);
    Dataset<Record> outputPart0 = outputDataset.getPartition(key, true);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputPart0), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputPart0));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testPartitionedSourceAndTargetWritingToTopLevel() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = partitionStrategy.partitionKey(0);
    Dataset<Record> inputPart0 = inputDataset.getPartition(key, false);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0, GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputDataset));

    // check all records are in the correct partition
    Dataset<Record> outputPart0 = outputDataset.getPartition(key, false);
    Assert.assertNotNull(outputPart0);
    Assert.assertEquals(5, datasetSize(outputPart0));
  }
}
