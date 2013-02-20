package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class TestMultiFileDatasetReader {

  private FileSystem fileSystem;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource("string.avsc")
        .openStream());
  }

  @Test
  public void test() throws IOException {
    Path testFile = new Path(Resources.getResource("strings-100.avro")
        .getFile());

    MultiFileDatasetReader<String> reader = new MultiFileDatasetReader<String>(
        fileSystem, Lists.newArrayList(testFile, testFile), testSchema);

    int records = 0;

    try {
      reader.open();

      while (reader.hasNext()) {
        String record = reader.read();
        Assert.assertNotNull(record);
        Assert.assertEquals("test-" + records % 100, record);
        records++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(200, records);
  }

}
