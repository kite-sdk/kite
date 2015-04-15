/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi.filesystem;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;

public class TestFileSystemDatasets {

  private static final Schema schema = SchemaBuilder.record("Event").fields()
      .requiredString("id")
      .requiredLong("timestamp")
      .requiredString("color")
      .endRecord();

  private static PartitionStrategy ymd = new PartitionStrategy.Builder()
      .year("timestamp", "y")
      .month("timestamp", "m")
      .day("timestamp", "d")
      .build();

  private FileSystemDataset<GenericRecord> dataset;

  @Before
  public void createFileSystemDataset() {
    String uri = "dataset:file:/tmp/datasets/ns/test";
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(ymd)
        .build();
    Datasets.delete(uri);
    this.dataset = Datasets.create(uri, descriptor);
  }

  @Test
  public void testViewForUri() {
    Path path = new Path("/tmp/datasets/ns/test/y=2014/m=03/d=14");

    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, "file:" + path);
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    view = FileSystemDatasets.viewForUri(
        dataset, path.toString());
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3).with("d", 14)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testViewForIncompleteUri() {
    Path path = new Path("/tmp/datasets/ns/test/y=2014/m=03");

    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, path.toString());
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testIgnoresAuthority() {
    Path path = new Path("/tmp/datasets/ns/test/y=2014/m=03/d=14");

    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, "file://127.0.0.1/tmp/datasets/ns/test/y=2014/m=03/d=14");
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3).with("d", 14)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testViewForRelativeUri() {
    Path path = new Path("/tmp/datasets/ns/test/y=2014/m=03/d=14");

    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, "y=2014/m=03/d=14");
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3).with("d", 14)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testViewForMissingPartitionNames() {
    Path path = new Path("2014/3/14");

    // like PathConversion, this uses names from the partition strategy
    // and will accept partitions that don't have a "name=" component
    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, path.toString());
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3).with("d", 14)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testViewForDifferentPartitionNames() {
    Path path = new Path("year=2014/month=3/day=14");

    // like PathConversion, this uses names from the partition strategy
    // and will accept partitions that have a different "name=" component
    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, path.toString());
    Assert.assertEquals("Should create correct view",
        view, dataset.getPartitionView(path));

    Constraints expected = ((AbstractRefinableView<GenericRecord>)
        dataset.with("y", 2014).with("m", 3).with("d", 14)).getConstraints();
    Constraints actual = ((AbstractRefinableView<GenericRecord>) view)
        .getConstraints();
    Assert.assertEquals("Constraints should match expected",
        expected, actual);
  }

  @Test
  public void testNoConstraints() {
    View<GenericRecord> view = FileSystemDatasets.viewForUri(
        dataset, "file:/tmp/datasets/ns/test/");
    Assert.assertEquals("Should create correct view", view, dataset);
  }

  @Test
  public void testDatasetNotPartitioned() {
    Datasets.delete("dataset:file:/tmp/datasets/ns/test");
    final Dataset<GenericRecord> ds = Datasets.create(
        "dataset:file:/tmp/datasets/ns/test",
        new DatasetDescriptor.Builder()
            .schema(schema)
            .build());

    Assert.assertEquals("Should work for empty relative directory",
        ds, FileSystemDatasets.viewForUri(ds, "file:/tmp/datasets/ns/test"));

    TestHelpers.assertThrows("Should reject paths in a non-partitioned dataset",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            FileSystemDatasets.viewForUri(ds, "y=2014/m=03/d=14");
          }
        });
  }

  @Test
  public void testNotContained() {
    TestHelpers.assertThrows("Should reject paths not in the dataset",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            FileSystemDatasets.viewForUri(
                dataset, "file:/tmp/datasets/ns/test2/y=2014/m=03/d=14");
          }
        });
  }

  @Test
  public void testOtherFileSystemRejected() {
    TestHelpers.assertThrows("Should reject paths not in the dataset",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            FileSystemDatasets.viewForUri(
                dataset, "hdfs:/tmp/datasets/ns/test/y=2014/m=03/d=14");
          }
        });
  }
}
