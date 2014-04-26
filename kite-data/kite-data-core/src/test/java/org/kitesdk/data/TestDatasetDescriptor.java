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
package org.kitesdk.data;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.STRING_SCHEMA;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.USER_SCHEMA_URL;

public class TestDatasetDescriptor extends MiniDFSTest {

  @Test
  public void testSchemaFromHdfs() throws IOException {
    FileSystem fs = getDFS();

    // copy a schema to HDFS
    Path schemaPath = fs.makeQualified(new Path("schema.avsc"));
    FSDataOutputStream out = fs.create(schemaPath);
    IOUtils.copyBytes(USER_SCHEMA_URL.toURL().openStream(), out, fs.getConf());
    out.close();

    // build a schema using the HDFS path and check it's the same
    Schema schema = new DatasetDescriptor.Builder().schemaUri(schemaPath.toUri()).build()
        .getSchema();

    Assert.assertEquals(USER_SCHEMA, schema);
  }

  @Test
  public void testSchemaFromAvroDataFile() throws Exception {
    URI uri = Resources.getResource("data/strings-100.avro").toURI();
    Schema schema = new DatasetDescriptor.Builder().schemaFromAvroDataFile(uri).build()
        .getSchema();
    Assert.assertEquals(STRING_SCHEMA, schema);
  }

  @Test
  public void testSchemaFromResourceURI() throws Exception {
    String uri = "resource:standard_event.avsc";
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schemaUri(uri).build();

    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(descriptor.getSchema());
  }

  @Test
  public void testEmbeddedPartitionStrategy() {
    Schema schema = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"partitions\": [" +
        "    {\"type\": \"hash\", \"source\": \"username\", \"buckets\": 16}," +
        "    {\"type\": \"identity\", \"source\": \"username\", \"name\": \"u\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    Assert.assertTrue("Descriptor should have partition strategy",
        descriptor.isPartitioned());

    PartitionStrategy expected = new PartitionStrategy.Builder()
        .hash("username", 16)
        .identity("username", "u", Object.class, -1)
        .build();
    Assert.assertEquals(expected, descriptor.getPartitionStrategy());
  }

  @Test
  public void testEmbeddedColumnMapping() {
    Schema schema = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"partitions\": [" +
        "    {\"type\": \"identity\", \"source\": \"id\", \"name\": \"id_copy\"}" +
        "  ]," +
        "  \"mapping\": [" +
        "    {\"type\": \"key\", \"source\": \"id\"}," +
        "    {\"type\": \"column\"," +
        "     \"source\": \"username\"," +
        "     \"family\": \"u\"," +
        "     \"qualifier\": \"username\"}," +
        "    {\"type\": \"column\"," +
        "     \"source\": \"real_name\"," +
        "     \"family\": \"u\"," +
        "     \"qualifier\": \"name\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}" +
        "  ]" +
        "}");

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    Assert.assertTrue("Descriptor should have partition strategy",
        descriptor.isPartitioned());

    ColumnMappingDescriptor expected = new ColumnMappingDescriptor.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMappingDescriptor());
  }
}
