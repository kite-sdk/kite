package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.data.DatasetWriter;
import com.cloudera.data.Partition;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestHDFSPartition {

  private Path testDirectory;
  private FileSystem fileSystem;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource("string.avsc")
        .openStream());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testWriter() throws IOException {
    Partition<String> partition = new HDFSPartition.Builder<String>()
        .directory(testDirectory).fileSystem(fileSystem).schema(testSchema)
        .get();

    Assert.assertNotNull(partition);

    DatasetWriter<String> writer = null;

    try {
      writer = partition.getWriter();

      writer.open();

      writer.write("test");
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

}
