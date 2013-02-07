package com.cloudera.data.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestHDFSDatasetWriter {

  private File testDirectory;
  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    testDirectory = Files.createTempDir();

    Configuration conf = new Configuration();

    conf.set("fs.default.name", "file:///");
    fileSystem = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(new Path(testDirectory.getAbsolutePath()), true);
  }

  @Test
  public void testWrite() throws IOException {
    HDFSDatasetWriter<String> writer = new HDFSDatasetWriter<String>(
        fileSystem, new Path(testDirectory.getAbsolutePath(), "write-1.avro"),
        Schema.create(Type.STRING));

    writer.open();

    for (int i = 0; i < 100; i++) {
      writer.write("entry " + i);

      if (i % 10 == 0) {
        writer.flush();
      }
    }

    writer.close();
  }

}
