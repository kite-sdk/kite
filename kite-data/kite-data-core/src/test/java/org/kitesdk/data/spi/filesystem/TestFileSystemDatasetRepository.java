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

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.TestDatasetRepositories;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.MetadataProvider;
import java.io.IOException;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.checkTestUsers;
import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.writeTestUsers;
import org.apache.avro.generic.GenericData.Record;

public class TestFileSystemDatasetRepository extends TestDatasetRepositories {

  public TestFileSystemDatasetRepository(boolean distributed) {
    super(distributed);
  }

  @Override
  @SuppressWarnings("deprecation")
  public DatasetRepository newRepo(MetadataProvider provider) {
    // this purposely does not set the Configuration to test that the code
    // relies on filesystem URIs set in the DatasetDescriptor.
    return new FileSystemDatasetRepository(
        new Configuration(), testDirectory, provider);
  }

  @Test
  public void testCreatePath() throws IOException {
    Dataset<Record> created = repo.create(NAMESPACE, NAME, testDescriptor);

    URI location = created.getDescriptor().getLocation();
    Assert.assertNotNull(
        "FileSystemDatasetRepository should return descriptor locations",
        location);
    Assert.assertTrue("Dataset data directory:" + location + " should exist",
        fileSystem.exists(new Path(location)));
  }

  @Test
  public void testLoadNewHasZeroSize() {
    ensureCreated();

    Dataset<Record> dataset = repo.load(NAMESPACE, NAME);

    /*
     * We perform a read test to make sure only data files are encountered
     * during a read.
     */
    Assert.assertEquals(0, DatasetTestUtilities.datasetSize(dataset));
  }

  @Test
  public void testUpdateFailsWithFormatChange() {
    Dataset<Record> dataset = repo.create(NAMESPACE, NAME,
        new DatasetDescriptor.Builder(testDescriptor)
            .format(Formats.AVRO)
            .build());

    DatasetDescriptor changed =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
        .format(Formats.PARQUET)
        .build();

    try {
      repo.update(NAMESPACE, NAME, changed);
      Assert.fail("Should fail due to format change");
    } catch (ValidationException e) {
      // expected
    }

    Assert.assertEquals(
        Formats.AVRO, repo.load(NAMESPACE, NAME).getDescriptor().getFormat());
  }

  @Test
  public void testUpdateFailsWithPartitionStrategyChange() {
    PartitionStrategy ps1 = new PartitionStrategy.Builder()
        .hash("username", 2)
        .build();
    PartitionStrategy ps2 = new PartitionStrategy.Builder()
        .hash("username", 2)
        .hash("email", 3)
        .build();

    Dataset<Record> dataset = repo.create(NAMESPACE, NAME,
        new DatasetDescriptor.Builder(testDescriptor)
            .partitionStrategy(ps1)
            .build());

    DatasetDescriptor changed =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .partitionStrategy(ps2)
            .build();

    try {
      repo.update(NAMESPACE, NAME, changed);
      Assert.fail("Should fail due to partition strategy change");
    } catch (ValidationException e) {
      // expected
    }

    Assert.assertEquals(
        ps1, repo.load(NAMESPACE, NAME).getDescriptor().getPartitionStrategy());
  }

  @Test
  public void testUpdateFailsWithLocationChange() {
    ensureCreated();
    Dataset<Record> dataset = repo.load(NAMESPACE, NAME);
    URI location = dataset.getDescriptor().getLocation();

    DatasetDescriptor changed =
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .location(new Path(testDirectory, "newDataLocation").toUri())
            .build();

    try {
      repo.update(NAMESPACE, NAME, changed);
      Assert.fail("Should fail due to data location change");
    } catch (ValidationException ex) {
      // expected
    }

    Assert.assertEquals(
        location, repo.load(NAMESPACE, NAME).getDescriptor().getLocation());
  }

  @Test
  public void testUpdateFailsWithIncompatibleSchemaChange() {
    Dataset<Record> dataset = repo.create(NAMESPACE, NAME, new DatasetDescriptor.Builder()
        .schema(testSchema).build());

    Assert.assertEquals("Dataset name is propagated", NAME,
        dataset.getName());
    Assert.assertEquals("Dataset schema is propagated", testSchema, dataset
        .getDescriptor().getSchema());

    Schema testSchemaV2 = SchemaBuilder.record("user").fields()
        .requiredString("username")
        .requiredString("email")
        .requiredString("favoriteColor") // incompatible - no default
        .endRecord();

    try {
      repo.update(NAMESPACE, NAME, new DatasetDescriptor.Builder().schema(testSchemaV2).build());
      Assert.fail("Should fail due to incompatible update");
    } catch (ValidationException e) {
      // expected
    }
    dataset = repo.load(NAMESPACE, NAME);
    Assert.assertEquals("Dataset schema is unchanged", testSchema, dataset
        .getDescriptor().getSchema());
  }

  @Test
  public void testUpdateSuccessfulWithCompatibleSchemaChangeFieldAdded() {
    Dataset<Record> dataset = repo.create(NAMESPACE, NAME, new DatasetDescriptor.Builder()
        .schema(testSchema).build());

    writeTestUsers(dataset, 5, 0, "email");
    checkTestUsers(dataset, 5, "email");

    Schema testSchemaV2 = SchemaBuilder.record("user").fields()
        .requiredString("username")
        .requiredString("email")
        .nullableString("favoriteColor", "orange")
        .endRecord();

    Dataset<Record> datasetV2 = repo.update(NAMESPACE, NAME,
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .schema(testSchemaV2)
            .build());

    Assert.assertEquals("Dataset schema is updated", testSchemaV2, datasetV2
        .getDescriptor().getSchema());

    // test that the old records can be read back with the new schema
    checkTestUsers(datasetV2, 5, "email", "favoriteColor");

    // write more users and test that the mixed set can be read back with the new schema
    writeTestUsers(datasetV2, 5, 5, "email", "favoriteColor");
    checkTestUsers(datasetV2, 10, "email", "favoriteColor");
  }

  @Test
  public void testUpdateSuccessfulWithCompatibleSchemaChangeFieldRemoved() {
    Dataset<Record> dataset = repo.create(NAMESPACE, NAME, new DatasetDescriptor.Builder()
        .schema(testSchema).build());

    writeTestUsers(dataset, 5, 0, "email");
    checkTestUsers(dataset, 5, "email");

    Schema testSchemaV2 = SchemaBuilder.record("user").fields()
        .requiredString("username")
        .endRecord();

    Dataset<Record> datasetV2 = repo.update(NAMESPACE, NAME,
        new DatasetDescriptor.Builder(dataset.getDescriptor())
            .schema(testSchemaV2)
            .build());

    Assert.assertEquals("Dataset schema is updated", testSchemaV2, datasetV2
        .getDescriptor().getSchema());

    // test that the old records can be read back with the new schema
    checkTestUsers(datasetV2, 5, new String[0]);

    // write more users and test that the mixed set can be read back with the new schema
    writeTestUsers(datasetV2, 5, 5, new String[0]);
    checkTestUsers(datasetV2, 10, new String[0]);
  }

  @Test
  public void testDeleteRemovesDatasetPath() throws IOException {
    ensureCreated();

    Dataset<Record> dataset = repo.load(NAMESPACE, NAME);
    Path dataPath = new Path(dataset.getDescriptor().getLocation());
    Assert.assertTrue(fileSystem.exists(dataPath));

    repo.delete(NAMESPACE, NAME);

    Assert.assertFalse(fileSystem.exists(dataPath));
  }
}
