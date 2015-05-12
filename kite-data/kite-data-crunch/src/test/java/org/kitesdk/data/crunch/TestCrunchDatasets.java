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
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.Signalable;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.PartitionedDataset;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.user.NewUserRecord;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.checkTestUsers;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.datasetSize;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.writeTestUsers;

@RunWith(Parameterized.class)
public abstract class TestCrunchDatasets extends MiniDFSTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    MiniDFSTest.setupFS();
    Object[][] data = new Object[][] {
        { getDFS() },
        { getFS() }
    };
    return Arrays.asList(data);
  }

  protected FileSystem fileSystem;
  protected Path testDirectory;
  private DatasetRepository repo;

  public TestCrunchDatasets(FileSystem fs) {
    this.fileSystem = fs;
    testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
  }

  abstract public DatasetRepository newRepo();

  @Before
  public void setUp() throws Exception {
    this.repo = newRepo();
    repo.delete("ns", "in");
    repo.delete("ns", "out");
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testGeneric() throws IOException {
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkTestUsers(outputDataset, 10);
  }

  @Test
  public void testGenericParquet() throws IOException {
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());

    // write two files, each of 5 records
    writeTestUsers(inputDataset, 5, 0);
    writeTestUsers(inputDataset, 5, 5);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    checkTestUsers(outputDataset, 10);
  }

  @Test
  public void testPartitionedSource() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = new PartitionKey(0);
    Dataset<Record> inputPart0 =
        ((PartitionedDataset<Record>) inputDataset).getPartition(key, false);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputDataset));
  }

  @Test
  public void testPartitionedSourceAndTarget() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = new PartitionKey(0);
    Dataset<Record> inputPart0 =
        ((PartitionedDataset<Record>) inputDataset).getPartition(key, false);
    Dataset<Record> outputPart0 =
        ((PartitionedDataset<Record>) outputDataset).getPartition(key, true);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0));
    pipeline.write(data, CrunchDatasets.asTarget(outputPart0), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputPart0));
  }

  @Test
  public void testPartitionedSourceAndTargetWritingToTopLevel() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    PartitionKey key = new PartitionKey(0);
    Dataset<Record> inputPart0 =
        ((PartitionedDataset<Record>) inputDataset).getPartition(key, false);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputPart0));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(5, datasetSize(outputDataset));

    // check all records are in the correct partition
    Dataset<Record> outputPart0 =
        ((PartitionedDataset<Record>) outputDataset).getPartition(key, false);
    Assert.assertNotNull(outputPart0);
    Assert.assertEquals(5, datasetSize(outputPart0));
  }

  @Test
  public void testSourceView() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).format(Formats.PARQUET).build());

    writeTestUsers(inputDataset, 10);

    View<Record> inputView = inputDataset.with("username", "test-0");
    Assert.assertEquals(1, datasetSize(inputView));

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputView));
    pipeline.write(data, CrunchDatasets.asTarget(outputDataset), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(1, datasetSize(outputDataset));
  }

  @Test
  public void testTargetView() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    View<Record> inputView = inputDataset.with("username", "test-0");
    Assert.assertEquals(1, datasetSize(inputView));
    View<Record> outputView = outputDataset.with("username", "test-0");

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputView));
    pipeline.write(data, CrunchDatasets.asTarget(outputView), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(1, datasetSize(outputDataset));
  }

    @Test
    public void testTargetViewProvidedPartition() throws IOException {
        PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().provided("version").build();

        Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
        Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

        View<Record> inputView = inputDataset.with("version", "test-version-0");

        writeTestUsers(inputView, 1);

        Assert.assertEquals(1, datasetSize(inputView));
        View<Record> outputView = outputDataset.with("version", "test-version-0");

        Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
        PCollection<GenericData.Record> data = pipeline.read(
                CrunchDatasets.asSource(inputView));
        pipeline.write(data, CrunchDatasets.asTarget(outputView), Target.WriteMode.APPEND);
        pipeline.run();

        Assert.assertEquals(1, datasetSize(outputDataset));
    }

  
  @Test
  public void testViewUris() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    URI sourceViewUri = new URIBuilder(repo.getUri(), "ns", "in").with("username",
        "test-0").build();
    View<Record> inputView = Datasets.<Record, Dataset<Record>> load(sourceViewUri,
        Record.class);
    Assert.assertEquals(1, datasetSize(inputView));
    
    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(CrunchDatasets
        .asSource(sourceViewUri, GenericData.Record.class));
    URI targetViewUri = new URIBuilder(repo.getUri(), "ns", "out").with(
        "email", "email-0").build();
    pipeline.write(data, CrunchDatasets.asTarget(targetViewUri),
        Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(1, datasetSize(outputDataset));
  }
  
  @Test
  public void testDatasetUris() throws IOException {
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash(
        "username", 2).build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 10);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(new URIBuilder(repo.getUri(), "ns", "in").build(),
            GenericData.Record.class));
    pipeline.write(data, CrunchDatasets.asTarget(
        new URIBuilder(repo.getUri(), "ns", "out").build()), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(10, datasetSize(outputDataset));
  }

  @Test(expected = CrunchRuntimeException.class)
  public void testWriteModeDefaultFailsWithExisting() throws IOException {
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    writeTestUsers(inputDataset, 1, 0);
    writeTestUsers(outputDataset, 1, 0);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset));
    pipeline.write(data, CrunchDatasets.asTarget((View<Record>) outputDataset));
  }

  @Test
  public void testWriteModeOverwrite() throws IOException {
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    writeTestUsers(inputDataset, 1, 0);
    writeTestUsers(outputDataset, 1, 1);

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputDataset));
    pipeline.write(data, CrunchDatasets.asTarget((View<Record>) outputDataset),
        Target.WriteMode.OVERWRITE);

    pipeline.run();

    checkTestUsers(outputDataset, 1);
  }

  @Test
  public void testWriteModeCheckpoint() throws Exception {
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    writeTestUsers(inputDataset, 1, 0);

    Thread.sleep(1000); // ensure output is newer than input on local filesystems with 1s granularity
    runCheckpointPipeline(inputDataset, outputDataset);

    // under hadoop1 the issues with LocalJobRunner (MAPREDUCE-2350) require that we
    // manually ready the output dataset
    if (Hadoop.isHadoop1()) {
      ((Signalable)outputDataset).signalReady();
    }

    checkTestUsers(outputDataset, 1);

    long lastModified = ((LastModifiedAccessor) outputDataset).getLastModified();

    // re-run without changing input and output should not change
    runCheckpointPipeline(inputDataset, outputDataset);
    checkTestUsers(outputDataset, 1);
    Assert.assertEquals(lastModified, ((LastModifiedAccessor) outputDataset).getLastModified());

    // re-write input then re-run and output should be re-written
    Thread.sleep(1000); // ensure new input is newer than output
    repo.delete("ns", "in");
    inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());
    writeTestUsers(inputDataset, 1, 0);
    runCheckpointPipeline(inputDataset, outputDataset);

    checkTestUsers(outputDataset, 1);
    Assert.assertTrue(((LastModifiedAccessor) outputDataset).getLastModified() > lastModified);
  }

  @Test
  public void testWriteModeCheckpointToNotReadyOutput() throws Exception {
    //identity partition so we can overwrite the output
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().
      identity("username").build();

    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.
        Builder().schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());
    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.
        Builder().schema(USER_SCHEMA).partitionStrategy(partitionStrategy).build());

    writeTestUsers(inputDataset, 1, 0);

    // ensure output is newer than input on local filesystems with 1s granularity
    Thread.sleep(1000);

    runCheckpointPipeline(inputDataset, outputDataset);

    checkTestUsers(outputDataset, 1);

    // under hadoop1 the issues with LocalJobRunner (MAPREDUCE-2350) require that we
    // manually ready the output dataset
    if (Hadoop.isHadoop1()) {
      ((Signalable)outputDataset).signalReady();
    } else {
      //under hadoop2 the output will have been marked ready
      Assert.assertTrue("output dataset should be ready after mapreduce", ((Signalable)outputDataset).isReady());
    }

    long lastModified = ((LastModifiedAccessor) outputDataset).getLastModified();

    // ensure output is newer than input on local filesystems with 1s granularity
    Thread.sleep(1000);

    // now output to a view, this ensures that the view isn't ready
    View<Record> outputView = outputDataset.with("username", "test-0");

    // re-run without changing input and output should change since the view is not ready
    runCheckpointPipeline(inputDataset, outputView);
    checkTestUsers(outputDataset, 1);
    Assert.assertTrue(((LastModifiedAccessor) outputView).getLastModified() > lastModified);
  }

  // Statically typed identify function to ensure the expected record is used.
  static class UserRecordIdentityFn extends MapFn<NewUserRecord, NewUserRecord> {

    @Override
    public NewUserRecord map(NewUserRecord input) {
      return input;
    }
  }

  @Test
     public void testUseReaderSchema() throws IOException {

    // Create a schema with only a username, so we can test reading it
    // with an enhanced record structure.
    Schema oldRecordSchema = SchemaBuilder.record("org.kitesdk.data.user.OldUserRecord")
        .fields()
        .requiredString("username")
        .endRecord();

    // create the dataset
    Dataset<Record> in = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(oldRecordSchema).build());
    Dataset<Record> out = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(oldRecordSchema).build());
    Record oldUser = new Record(oldRecordSchema);
    oldUser.put("username", "user");

    DatasetWriter<Record> writer = in.newWriter();

    try {

      writer.write(oldUser);

    } finally {
      writer.close();
    }

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);

    // read data from updated dataset that has the new schema.
    // At this point, User class has the old schema
    PCollection<NewUserRecord> data = pipeline.read(CrunchDatasets.asSource(in.getUri(),
        NewUserRecord.class));

    PCollection<NewUserRecord> processed = data.parallelDo(new UserRecordIdentityFn(),
        Avros.records(NewUserRecord.class));

    pipeline.write(processed, CrunchDatasets.asTarget(out));

    DatasetReader reader = out.newReader();

    Assert.assertTrue("Pipeline failed.", pipeline.run().succeeded());

    try {

      // there should be one record that is equal to our old user generic record.
      Assert.assertEquals(oldUser, reader.next());
      Assert.assertFalse(reader.hasNext());

    } finally {
      reader.close();
    }
  }

  @Test
  public void testUseReaderSchemaParquet() throws IOException {

    // Create a schema with only a username, so we can test reading it
    // with an enhanced record structure.
    Schema oldRecordSchema = SchemaBuilder.record("org.kitesdk.data.user.OldUserRecord")
        .fields()
        .requiredString("username")
        .endRecord();

    // create the dataset
    Dataset<Record> in = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .format(Formats.PARQUET).schema(oldRecordSchema).build());

    Dataset<Record> out = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .format(Formats.PARQUET).schema(oldRecordSchema).build());
    Record oldUser = new Record(oldRecordSchema);
    oldUser.put("username", "user");

    DatasetWriter<Record> writer = in.newWriter();

    try {

      writer.write(oldUser);

    } finally {
      writer.close();
    }

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);

    // read data from updated dataset that has the new schema.
    // At this point, User class has the old schema
    PCollection<NewUserRecord> data = pipeline.read(CrunchDatasets.asSource(in.getUri(),
        NewUserRecord.class));

    PCollection<NewUserRecord> processed = data.parallelDo(new UserRecordIdentityFn(),
        Avros.records(NewUserRecord.class));

    pipeline.write(processed, CrunchDatasets.asTarget(out));

    DatasetReader reader = out.newReader();

    Assert.assertTrue("Pipeline failed.", pipeline.run().succeeded());

    try {

      // there should be one record that is equal to our old user generic record.
      Assert.assertEquals(oldUser, reader.next());
      Assert.assertFalse(reader.hasNext());

    } finally {
      reader.close();
    }
  }

  @Test
  public void testSignalReadyOutputView() {
    Assume.assumeTrue(!Hadoop.isHadoop1());
    Dataset<Record> inputDataset = repo.create("ns", "in", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    Dataset<Record> outputDataset = repo.create("ns", "out", new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA).build());

    writeTestUsers(inputDataset, 10);

    View<Record> inputView = inputDataset.with("username", "test-8", "test-9");
    View<Record> outputView = outputDataset.with("username", "test-8", "test-9");
    Assert.assertEquals(2, datasetSize(inputView));

    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputView));
    pipeline.write(data, CrunchDatasets.asTarget(outputView), Target.WriteMode.APPEND);
    pipeline.run();

    Assert.assertEquals(2, datasetSize(outputView));

    Assert.assertFalse("Output dataset should not be signaled ready",
        ((Signalable)outputDataset).isReady());
    Assert.assertTrue("Output view should be signaled ready",
        ((Signalable)outputView).isReady());
  }

  private void runCheckpointPipeline(View<Record> inputView,
      View<Record> outputView) {
    Pipeline pipeline = new MRPipeline(TestCrunchDatasets.class);
    PCollection<GenericData.Record> data = pipeline.read(
        CrunchDatasets.asSource(inputView));
    pipeline.write(data, CrunchDatasets.asTarget(outputView),
        Target.WriteMode.CHECKPOINT);
    pipeline.done();
  }
}
