package com.cloudera.data.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.data.DatasetReader;
import com.google.common.io.Resources;

public class TestHDFSDatasetReader {

  private FileSystem fileSystem;
  private Schema testSchema;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testSchema = new Schema.Parser().parse(Resources.getResource("string.avsc")
        .openStream());
  }

  @Test
  public void testRead() throws IOException {
    DatasetReader<String> reader = null;
    int records = 0;

    try {
      reader = new HDFSDatasetReader<String>(fileSystem, new Path(Resources
          .getResource("strings-100.avro").getFile()), testSchema);

      reader.open();

      while (reader.hasNext()) {
        String record = reader.read();

        Assert.assertNotNull(record);
        records++;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    Assert.assertEquals(100, records);
  }

}
