/*
 * Copyright 2015 Cloudera, Inc.
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
package org.kitesdk.data.mapreduce;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Formats;
import static org.kitesdk.data.mapreduce.FileSystemTestBase.STATS_SCHEMA;
import org.kitesdk.data.spi.DefaultConfiguration;

public class TestMergeOutputCommitter extends FileSystemTestBase {

  private Dataset<GenericData.Record> outputDataset;

  public TestMergeOutputCommitter() {
    super(Formats.AVRO);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    outputDataset = repo.create("ns", "out",
      new DatasetDescriptor.Builder()
      .property("kite.allow.csv", "true")
      .schema(STATS_SCHEMA)
      .format(format)
      .build(), GenericData.Record.class);
  }

  @After
  public void tearDown() {
    repo.delete("ns", "out");
  }

  @Test
  public void testSetupJobIsIdempotent() {
    DatasetKeyOutputFormat.MergeOutputCommitter<Object> outputCommitter
      = new DatasetKeyOutputFormat.MergeOutputCommitter<Object>();

    Configuration conf = DefaultConfiguration.get();
    DatasetKeyOutputFormat.configure(conf).appendTo(outputDataset);

    JobID jobId = new JobID("jt", 42);
    JobContext context = Hadoop.JobContext.ctor.newInstance(conf, jobId);

    // setup the job
    outputCommitter.setupJob(context);

    // call setup again to simulate an ApplicationMaster restart
    outputCommitter.setupJob(context);
  }

}
