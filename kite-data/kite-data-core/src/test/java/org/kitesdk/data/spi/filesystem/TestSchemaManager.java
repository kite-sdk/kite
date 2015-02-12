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
package org.kitesdk.data.spi.filesystem;

import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.MiniDFSTest;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestSchemaManager extends MiniDFSTest {

  Path testDirectory;
  Configuration conf;
  FileSystem fileSystem;

  boolean distributed;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
            { false },  // default to local FS
            { true } }; // default to distributed FS
    return Arrays.asList(data);
  }

  public TestSchemaManager(boolean distributed) {
    this.distributed = distributed;
  }

  @Before
  public void setup() throws IOException {

    this.conf = (distributed ?
            MiniDFSTest.getConfiguration() :
            new Configuration());

    this.fileSystem = FileSystem.get(conf);

    this.testDirectory = fileSystem.makeQualified(
            new Path(Files.createTempDir().getAbsolutePath()));
  }


  @Test
  public void testCreateSchema() throws IOException {

    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);

    Schema schema = manager.getNewestSchema();

    Assert.assertEquals(DatasetTestUtilities.USER_SCHEMA, schema);
  }

  @Test
  public void testUpdateSchema() throws IOException {

    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);

    Schema schema = manager.getNewestSchema();

    Assert.assertEquals(DatasetTestUtilities.USER_SCHEMA, schema);

    // Create an updated schema and ensure it can be written.
    Schema updatedSchema = SchemaBuilder.record(schema.getName())
            .fields()
            .requiredString("username")
            .requiredString("email")
            .optionalBoolean("extra_field").endRecord();

    manager.writeSchema(updatedSchema);

    Assert.assertEquals(updatedSchema, manager.getNewestSchema());
  }

  @Test
  public void testManyUpdates() throws IOException {
    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    // Create an updated schema and ensure it can be written.
    for (int i = 0; i < 20; ++i) {

      SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
          .record("test").fields();

      for (int j = 0; j <= i; ++j) {
        fields.optionalString("field_" + j);
      }

      Schema schema = fields.endRecord();

      manager.writeSchema(schema);

      // Ensure we always see the newest schema on load.
      Assert.assertEquals(schema, manager.getNewestSchema());
    }

    // Make sure all of the updates are in place.
    Map<Integer, Schema> schemas = manager.getSchemas();

    Assert.assertEquals(20, schemas.size());
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testIncompatibleUpdate() {
    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    // Trivially incompatible schemas should yield an exception.
    manager.writeSchema(SchemaBuilder.record("test")
        .fields()
        .requiredString("foo")
        .endRecord());

    manager.writeSchema(SchemaBuilder.record("test")
        .fields()
        .requiredString("bar")
        .endRecord());
  }

  @Test(expected = IncompatibleSchemaException.class)
  public void testIndirectIncompatibleUpdate() {
    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    // Write two schemas that are compatible since they use optional fields.
    manager.writeSchema(SchemaBuilder.record("test")
        .fields()
        .optionalString("foo")
        .endRecord());

    manager.writeSchema(SchemaBuilder.record("test")
        .fields()
        .optionalString("bar")
        .endRecord());

    // This schema creates a schema compatible with the immediately previous
    // version, but incompatible with the original.
    manager.writeSchema(SchemaBuilder.record("test")
        .fields()
        .optionalInt("foo")
        .endRecord());
  }

  @Test
  public void testNoSchemaManagerDirectory() throws IOException {

    SchemaManager manager = SchemaManager.load(getConfiguration(),
            new Path(testDirectory, "NO_SUCH_DIRECTORY"));

    Assert.assertNull(manager);
  }

  @Test
  public void testSameSchemaUpdate() throws IOException {

    SchemaManager manager = SchemaManager.create(getConfiguration(), testDirectory);

    URI uri1 = manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);
    URI uri2 = manager.writeSchema(DatasetTestUtilities.USER_SCHEMA);

    Assert.assertEquals("Updating with the same schema should not create a new URI",
        uri1, uri2);
  }
}
