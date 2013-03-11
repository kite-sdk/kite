package com.cloudera.data.filesystem;

import com.cloudera.data.DatasetDescriptor;
import com.google.common.io.Resources;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class TestDatasetDescriptor {

  @Test
  public void testSchemaFromAvroDataFile() throws IOException {
    URL url = Resources.getResource("data/strings-100.avro");
    Schema schema = new DatasetDescriptor.Builder().schemaFromAvroDataFile(url).get()
        .getSchema();
    Schema expectedSchema = new Schema.Parser().parse(
        Resources.getResource("schema/string.avsc").openStream());
    Assert.assertEquals(expectedSchema, schema);
  }
}
