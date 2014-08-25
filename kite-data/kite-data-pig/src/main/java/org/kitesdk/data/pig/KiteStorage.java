/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.pig;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.avro.AvroStorageDataConversionUtilities;
import org.apache.pig.impl.util.avro.AvroTupleWrapper;
import org.apache.pig.piggybank.storage.avro.AvroSchema2Pig;
import org.apache.pig.piggybank.storage.avro.PigSchema2Avro;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.View;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.SchemaValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KiteStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(KiteStorage.class);

  private View<GenericRecord> inputDataset = null;
  private View<GenericRecord> outputDataset = null;
  private RecordReader<GenericRecord, Void> recordReader = null;
  private RecordWriter<GenericRecord, Void> recordWriter = null;
  private Job job = null;
  private Schema outputSchema = null;

  @Override
  public void setLocation(String location, Job job) throws IOException {
    inputDataset = Datasets.load(fixLocation(location));
    DatasetKeyInputFormat.configure(job).readFrom(inputDataset);
    this.job = job;
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    DatasetKeyInputFormat<GenericRecord> input = new DatasetKeyInputFormat<GenericRecord>();
    input.setConf(job.getConfiguration());
    return input;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    recordReader = reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!recordReader.nextKeyValue()) {
        return null;
      }
      GenericRecord record = recordReader.getCurrentKey();
      return new AvroTupleWrapper<GenericRecord>(record);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
    return relativeToAbsolutePath(location, curDir);
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    String[] parts = location.split(":", 2);
    if (parts[1].charAt(0) != '/') {
      return parts[0] + ":/" + parts[1];
    }

    return location;
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new DatasetKeyOutputFormat<GenericRecord>();
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    outputDataset = Datasets.load(fixLocation(location));
    validateSchema(outputDataset.getDataset().getDescriptor().getSchema());
    DatasetKeyOutputFormat.configure(job).writeTo(outputDataset);
    this.job = job;
  }

  private void validateSchema(Schema datasetSchema) {
    if (outputSchema == null) {
      return;
    }

    if (!SchemaValidationUtil.canRead(outputSchema, datasetSchema)) {
      throw new DatasetException(
          String.format("Trying to write with incompatible schema. Output "
              + "schema:%n%n%s%n%n is incompatible with dataset schema:"
              + "%n%n%s", outputSchema.toString(true),
              datasetSchema.toString(true)));
    }
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    outputSchema = PigSchema2Avro.convert(s, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    recordWriter = writer;
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    try {
      GenericData.Record record = AvroStorageDataConversionUtilities.packIntoAvro(t,
          outputDataset.getDataset().getDescriptor().getSchema());
      recordWriter.write(record, null);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
  }

  @Override
  public void cleanupOnSuccess(String location, Job job) throws IOException {
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    View<GenericRecord> view = Datasets.load(fixLocation(location));
    return AvroSchema2Pig.convert(view.getDataset().getDescriptor().getSchema());
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    return null;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS")
  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
  }

  public static String fixLocation(String location) {
    String[] parts = location.split(":", 2);
    if (parts[1].charAt(0) == '/') {
      return parts[0] + ":" + parts[1].substring(1);
    }
    return location;
  }
}
