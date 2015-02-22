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
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities;

public class TestDatasetDescriptor {

  private static final Schema USER_SCHEMA = SchemaBuilder.record("User")
      .fields()
      .requiredLong("id")
      .requiredString("name")
      .requiredString("email")
      .requiredLong("version")
      .requiredLong("visit_count")
      .name("custom_attributes").type(
          SchemaBuilder.map().values().stringType()).noDefault()
      .name("preferences").type(
          SchemaBuilder.record("Preferences").fields()
              .requiredBoolean("text_email")
              .requiredString("time_zone")
              .endRecord())
          .noDefault()
      .name("posts").type(SchemaBuilder.array().items().longType()).noDefault()
      .endRecord();

  @Test
  public void testSchemaFromHdfs() throws IOException {
    MiniDFSTest.setupFS();
    FileSystem fs = MiniDFSTest.getDFS();

    // copy a schema to HDFS
    Path schemaPath = fs.makeQualified(new Path("schema.avsc"));
    FSDataOutputStream out = fs.create(schemaPath);
    IOUtils.copyBytes(DatasetTestUtilities.USER_SCHEMA_URL.toURL().openStream(),
        out, fs.getConf());
    out.close();

    // build a schema using the HDFS path and check it's the same
    Schema schema = new DatasetDescriptor.Builder().schemaUri(schemaPath.toUri()).build()
        .getSchema();

    Assert.assertEquals(DatasetTestUtilities.USER_SCHEMA, schema);
    MiniDFSTest.teardownFS();
  }

  @Test
  public void testSchemaFromAvroDataFile() throws Exception {
    URI uri = Resources.getResource("data/strings-100.avro").toURI();
    Schema schema = new DatasetDescriptor.Builder().schemaFromAvroDataFile(uri).build()
        .getSchema();
    Assert.assertEquals(DatasetTestUtilities.STRING_SCHEMA, schema);
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
        .identity("username", "u")
        .build();
    Assert.assertEquals(expected, descriptor.getPartitionStrategy());

    // check that strategies set on the builder override those in the schema
    expected = new PartitionStrategy.Builder()
        .identity("real_name", "n")
        .build();
    DatasetDescriptor override = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(expected)
        .build();
    Assert.assertEquals(expected, override.getPartitionStrategy());
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

    ColumnMapping expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());
  }

  @Test
  public void testCopyUsesEmbeddedColumnMapping() {
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

    ColumnMapping expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());

    schema = new Schema.Parser().parse("{" +
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
        "     \"qualifier\": \"name\"}," +
        "    {\"type\": \"column\"," +
        "     \"source\": \"age\"," +
        "     \"family\": \"u\"," +
        "     \"qualifier\": \"age\"}" +
        "  ]," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\"}," +
        "    {\"name\": \"username\", \"type\": \"string\"}," +
        "    {\"name\": \"real_name\", \"type\": \"string\"}," +
        "    {\"name\": \"age\", \"type\": \"long\"}" +
        "  ]" +
        "}");

    descriptor = new DatasetDescriptor.Builder(descriptor)
        .schema(schema)
        .build();
    Assert.assertTrue("Descriptor should have partition strategy",
        descriptor.isPartitioned());

    expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .column("age", "u", "age")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());
  }

  @Test
  public void testCopyUsesCopiedColumnMapping() {
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

    ColumnMapping expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());

    descriptor = new DatasetDescriptor.Builder(descriptor)
        .build();
    Assert.assertTrue("Descriptor should have partition strategy",
        descriptor.isPartitioned());

    expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());
  }

  @Test
  public void testEmbeddedFieldMappings() {
    Schema schema = new Schema.Parser().parse("{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"User\",\n" +
        "  \"partitions\": [\n" +
        "    {\"type\": \"identity\", \"source\": \"id\", \"name\": \"id_copy\"}\n" +
        "  ],\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"id\", \"type\": \"long\", \"mapping\": {\n" +
        "        \"type\": \"key\"\n" +
        "      } },\n" +
        "    {\"name\": \"username\", \"type\": \"string\", \"mapping\": {\n" +
        "        \"type\": \"column\", \"family\": \"u\",\n" +
        "        \"qualifier\": \"username\"\n" +
        "      } },\n" +
        "    {\"name\": \"real_name\", \"type\": \"string\", \"mapping\": {\n" +
        "        \"type\": \"column\", \"value\": \"u:name\"\n" +
        "      } }\n" +
        "  ]\n" +
        "}\n");
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    Assert.assertTrue("Descriptor should have partition strategy",
        descriptor.isPartitioned());

    ColumnMapping expected = new ColumnMapping.Builder()
        .key("id")
        .column("username", "u", "username")
        .column("real_name", "u", "name")
        .build();
    Assert.assertEquals(expected, descriptor.getColumnMapping());
  }

  @Test
  public void testPartitionSourceMustBeSchemaField() {
    TestHelpers.assertThrows("Should reject partition source not in schema",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .year("created_at")
                    .build())
                .build();
          }
        }
    );
  }

  @Test
  public void testMappingSourceMustBeSchemaField() {
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .columnMapping(new ColumnMapping.Builder()
            .column("id", "meta", "id")
            .build())
        .build());

    TestHelpers.assertThrows("Should reject mapping source not in schema",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .column("created_at", "meta", "created_at")
                    .build())
                .build();
          }
        });
  }

  @Test
  public void testKeyMappingSourceMustBeIdentityPartitioned() {
    // and it works when the field is present
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("id", 16)
            .identity("id")
            .build())
        .columnMapping(new ColumnMapping.Builder()
            .key("id")
            .build())
        .build());

    TestHelpers.assertThrows("Should reject mapping source not id partitioned",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .partitionStrategy(new PartitionStrategy.Builder()
                    .hash("id", 16)
                    .build())
                .columnMapping(new ColumnMapping.Builder()
                    .key("id")
                    .build())
                .build();
          }
        }
    );
  }

  @Test
  public void testCounterMappingSourceMustBeIntOrLong() {
    // works for a long field
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .columnMapping(new ColumnMapping.Builder()
            .counter("visit_count", "meta", "visits")
            .build())
        .build());

    TestHelpers.assertThrows("Should reject string mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .counter("email", "meta", "email")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject record mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .counter("custom_attributes", "meta", "attrs")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject map mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .counter("preferences", "meta", "prefs")
                    .build())
                .build();
          }
        }
    );
    TestHelpers.assertThrows("Should reject list mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .counter("posts", "meta", "post_ids")
                    .build())
                .build();
          }
        }
    );
  }

  @Test
  public void testVersionMappingSourceMustBeIntOrLong() {
    // works for a long field
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .columnMapping(new ColumnMapping.Builder()
            .version("version")
            .build())
        .build());

    TestHelpers.assertThrows("Should reject string mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .version("name")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject record mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .version("custom_attributes")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject map mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .version("preferences")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject list mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .version("posts")
                    .build())
                .build();
          }
        }
    );
  }

  @Test
  public void testKACMappingSourceMustBeRecordOrMap() {
    // works for a map field
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .columnMapping(new ColumnMapping.Builder()
            .keyAsColumn("custom_attributes", "attrs")
            .build())
        .build());
    // works for a record field
    Assert.assertNotNull(new DatasetDescriptor.Builder()
        .schema(USER_SCHEMA)
        .columnMapping(new ColumnMapping.Builder()
            .keyAsColumn("preferences", "prefs")
            .build())
        .build());

    TestHelpers.assertThrows("Should reject long mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .keyAsColumn("id", "kac")
                    .build())
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject string mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .keyAsColumn("email", "kac")
                    .build())
                .build();
          }
        }
    );
    TestHelpers.assertThrows("Should reject list mapping source",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            new DatasetDescriptor.Builder()
                .schema(USER_SCHEMA)
                .columnMapping(new ColumnMapping.Builder()
                    .keyAsColumn("posts", "kac")
                    .build())
                .build();
          }
        }
    );
  }

  @Test
  public void testBackwardCompatibleMappingToPartitionStrategy() {
    Schema schema = new Schema.Parser().parse("{" +
        "  \"type\": \"record\"," +
        "  \"name\": \"User\"," +
        "  \"fields\": [" +
        "    {\"name\": \"id\", \"type\": \"long\", \"mapping\":" +
        "      {\"type\": \"key\", \"value\": \"1\"} }," +
        "    {\"name\": \"username\", \"type\": \"string\", \"mapping\":" +
        "      {\"type\": \"key\", \"value\": \"0\"} }," +
        "    {\"name\": \"real_name\", \"type\": \"string\", \"mapping\":" +
        "      {\"type\": \"column\", \"value\": \"m:name\"} }" +
        "  ]" +
        "}");
    PartitionStrategy expected = new PartitionStrategy.Builder()
        .identity("username")
        .identity("id")
        .build();
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .build();
    Assert.assertEquals(expected, descriptor.getPartitionStrategy());
  }
}
