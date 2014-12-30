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
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.crunch.MapFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.cli.TestUtil;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestInputFormatImportCommandCluster extends MiniDFSTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private static Configuration original;
  private static String datasetUri = "dataset:hdfs:/tmp/datasets/sequence";

  private Logger console = null;
  private InputFormatImportCommand command;

  private static List<Measurement> measurements = Lists.newArrayList(
      new Measurement("temp", 32.0),
      new Measurement("speed", 88.0),
      new Measurement("length", 6.0)
  );

  @BeforeClass
  public static void replaceDefaultConfig() {
    original = DefaultConfiguration.get();
    DefaultConfiguration.set(getConfiguration());
  }

  @AfterClass
  public static void restoreDefaultConfig() {
    DefaultConfiguration.set(original);
  }

  @Before
  public void createTargetDataset() throws Exception {
    removeTargetDataset();
    String avsc = temp.newFile("schema.avsc").toString();
    TestUtil.run("obj-schema", Measurement.class.getName(), "-o", avsc);
    TestUtil.run("create", datasetUri, "-s", avsc);
  }

  @Before
  public void createCommand() {
    this.console = mock(Logger.class);
    this.command = new InputFormatImportCommand(console);
    // set up the configuration as it would be with a cluster
    Configuration conf = getConfiguration();
    conf.setBoolean("kite.testing", true);
    command.setConf(conf);
  }

  @After
  public void removeTargetDataset() throws Exception {
    TestUtil.run("delete", datasetUri);
  }

  @Test
  public void testLocalImport() throws Exception {
    String sample = temp.newFile("sample.sequence").toString();
    writeSequenceFile(getFS(), new Path(sample)); // local sequence file

    // Crunch will use a MemPipeline, so use the custom copying InputFormat
    command.inFormatClass = CopyingInputFormat.class.getName();
    command.targets = Lists.newArrayList(sample, datasetUri);
    command.noCompaction = true; // no need to run reducers

    int rc = command.run();

    Assert.assertEquals("Should return success", 0, rc);

    verify(console).info("Added {} records to \"{}\"", 3L, datasetUri);
    verifyNoMoreInteractions(console);

    Set<Measurement> datasetContent = materialize(
        Datasets.load(datasetUri, Measurement.class));
    Assert.assertEquals(Sets.newHashSet(measurements), datasetContent);
  }

  @Test
  public void testLocalImportWithTransform() throws Exception {
    String sample = temp.newFile("sample.sequence").toString();
    writeSequenceFile(getFS(), new Path(sample)); // local sequence file

    // Crunch will use a MemPipeline, so use the custom copying InputFormat
    command.inFormatClass = CopyingInputFormat.class.getName();
    command.targets = Lists.newArrayList(sample, datasetUri);
    command.noCompaction = true; // no need to run reducers
    command.transform = TransformMeasurement.class.getName();

    int rc = command.run();

    Assert.assertEquals("Should return success", 0, rc);

    verify(console).info("Added {} records to \"{}\"", 3L, datasetUri);
    verifyNoMoreInteractions(console);

    Set<Measurement> datasetContent = materialize(
        Datasets.load(datasetUri, Measurement.class));
    Set<Measurement> expected = Sets.newHashSet(Iterables.transform(
        measurements, new TransformMeasurement()));
    Assert.assertEquals(expected, datasetContent);
  }

  @Test
  public void testMRImport() throws Exception {
    Path sample = new Path(temp.newFile("sample.sequence").toString())
        .makeQualified(getDFS().getUri(), new Path("/"));
    writeSequenceFile(getDFS(), sample); // HDFS sequence file

    // Reusing records is okay when running in MR
    command.inFormatClass = SequenceFileInputFormat.class.getName();
    command.targets = Lists.newArrayList(sample.toString(), datasetUri);
    command.noCompaction = true; // no need to run reducers

    int rc = command.run();

    Assert.assertEquals("Should return success", 0, rc);

    verify(console).info("Added {} records to \"{}\"", 3L, datasetUri);
    verifyNoMoreInteractions(console);

    Set<Measurement> datasetContent = materialize(
        Datasets.load(datasetUri, Measurement.class));
    Assert.assertEquals(Sets.newHashSet(measurements), datasetContent);
  }

  @Test
  public void testMRImportWithTransform() throws Exception {
    Path sample = new Path(temp.newFile("sample.sequence").toString())
        .makeQualified(getDFS().getUri(), new Path("/"));
    writeSequenceFile(getDFS(), sample); // HDFS sequence file

    // Reusing records is okay when running in MR
    command.inFormatClass = SequenceFileInputFormat.class.getName();
    command.targets = Lists.newArrayList(sample.toString(), datasetUri);
    command.noCompaction = true; // no need to run reducers
    command.transform = TransformMeasurement.class.getName();

    int rc = command.run();

    Assert.assertEquals("Should return success", 0, rc);

    verify(console).info("Added {} records to \"{}\"", 3L, datasetUri);
    verifyNoMoreInteractions(console);

    Set<Measurement> datasetContent = materialize(
        Datasets.load(datasetUri, Measurement.class));
    Set<Measurement> expected = Sets.newHashSet(Iterables.transform(
        measurements, new TransformMeasurement()));
    Assert.assertEquals(expected, datasetContent);
  }

  @SuppressWarnings("deprecation") // compatible with hadoop-1
  private static void writeSequenceFile(FileSystem fs, Path path) throws Exception {
    SequenceFile.Writer writer = SequenceFile.createWriter(
        fs, getConfiguration(), path, Text.class, Measurement.class);
    for (Measurement m : measurements) {
      writer.append(new Text(m.name), m);
    }
    writer.close();
  }

  public static class TransformMeasurement
      extends MapFn<Measurement, Measurement>
      implements Function<Measurement, Measurement> {
    @Override
    public Measurement map(Measurement m) {
      return apply(m);
    }

    @Override
    public Measurement apply(Measurement m) {
      m.value += 3.0;
      return m;
    }
  }

  /**
   * This custom InputFormat is needed because objects are reused, but Crunch
   * accumulates objects when using a MemPipeline.
   */
  public static class CopyingInputFormat
      extends SequenceFileInputFormat<Text, Measurement> {
    @Override
    public RecordReader<Text, Measurement> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new CopyingRecordReader();
    }

    public static class CopyingRecordReader
        extends SequenceFileRecordReader<Text, Measurement> {
      @Override
      public Text getCurrentKey() {
        return new Text(super.getCurrentKey());
      }

      @Override
      public Measurement getCurrentValue() {
        return new Measurement(super.getCurrentValue());
      }
    }
  }

  public static class Measurement implements Writable {
    private String name;
    private double value;

    public Measurement() {
    }

    public Measurement(Measurement toCopy) {
      this.name = toCopy.name;
      this.value = toCopy.value;
    }

    public Measurement(String name, double value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(name.length());
      out.write(Charsets.UTF_8.encode(name).array());
      out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] nameBytes = new byte[length];
      in.readFully(nameBytes);
      this.name = Charsets.UTF_8.decode(ByteBuffer.wrap(nameBytes)).toString();
      this.value = in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Measurement that = (Measurement) o;
      return (Objects.equal(this.name, that.name) &&
          (Double.compare(this.value, that.value) == 0));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, value);
    }
  }

  /**
   * Copy records while materializing in case they are reused.
   */
  private static Set<Measurement> materialize(View<Measurement> view) {
    Set<Measurement> measurements = Sets.newHashSet();
    for (Measurement m : view.newReader()) {
      measurements.add(new Measurement(m));
    }
    return measurements;
  }
}
