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
package com.cloudera.data.filesystem;

import java.io.IOException;

import com.cloudera.data.Dataset;
import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.PartitionStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestPartitionedDatasetWriter {

  private Path testDirectory;
  private FileSystem fileSystem;
  private Schema testSchema;
  private FileSystemDatasetRepository repo;
  private PartitionedDatasetWriter<Object> writer;

  @Before
  public void setUp() throws IOException {
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource(
        "schema/user-partitioned.avsc").openStream());
    repo = new FileSystemDatasetRepository(fileSystem, testDirectory);
    PartitionStrategy partitionStrategy = new PartitionExpression(testSchema
        .getProp("cdk.partition.expression"), true).evaluate();
    Dataset users = repo.create(
        "users",
        new DatasetDescriptor.Builder()
            .schema(testSchema)
            .partitionStrategy(
                partitionStrategy)
            .get());
    writer = new PartitionedDatasetWriter<Object>(users, partitionStrategy);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testBasicOpenClose() throws IOException {
    writer.open();
    writer.close();
  }

  @Test
  public void testWriter() throws IOException {
    Record record = new GenericRecordBuilder(testSchema).set("username",
        "test1").build();

    try {
      writer.open();
      writer.write(record);
      writer.flush();
      writer.close();
    } finally {
      Closeables.close(writer, true);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteToClosedWriterFails() throws IOException {
    Record record = new GenericRecordBuilder(testSchema).set("username",
        "test1").build();
    writer.open();
    writer.close();
    writer.write(record);
  }

}
