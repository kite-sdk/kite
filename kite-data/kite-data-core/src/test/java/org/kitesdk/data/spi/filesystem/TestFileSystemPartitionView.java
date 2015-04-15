/*
 * Copyright 2015 Cloudera Inc.
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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.PartitionView;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.View;

public class TestFileSystemPartitionView {
  public static class TestRecord {
    private long id;
    private String data;
  }

  private FileSystemDataset<TestRecord> unpartitioned = null;
  private FileSystemDataset<TestRecord> partitioned = null;

  @Before
  public void createTestDatasets() {
    Datasets.delete("dataset:file:/tmp/datasets/unpartitioned");
    Datasets.delete("dataset:file:/tmp/datasets/partitioned");

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(TestRecord.class)
        .build();
    unpartitioned = Datasets.create("dataset:file:/tmp/datasets/unpartitioned",
        descriptor, TestRecord.class);

    descriptor = new DatasetDescriptor.Builder(descriptor)
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("id", 4)
            .build())
        .build();
    partitioned = Datasets.create("dataset:file:/tmp/datasets/partitioned",
        descriptor, TestRecord.class);

    writeTestRecords(unpartitioned);
    writeTestRecords(partitioned);
  }

  @After
  public void removeTestDatasets() {
    Datasets.delete("dataset:file:/tmp/datasets/unpartitioned");
    Datasets.delete("dataset:file:/tmp/datasets/partitioned");
  }

  @Test
  public void testFullPaths() {
    FileSystemPartitionView<TestRecord> partition = partitioned
        .getPartitionView(URI.create("file:/tmp/datasets/partitioned"));
    Assert.assertEquals("Should accept a full root URI",
        URI.create("file:/tmp/datasets/partitioned"),
        partition.getLocation());
    Assert.assertEquals("Should have a null relative URI",
        null, partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        new Path("file:/tmp/datasets/partitioned"));
    Assert.assertEquals("Should accept a full root Path",
        URI.create("file:/tmp/datasets/partitioned"),
        partition.getLocation());
    Assert.assertEquals("Should have a null relative Path",
        null, partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"));
    Assert.assertEquals("Should accept a full sub-partition URI",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have a correct relative URI",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        new Path("file:/tmp/datasets/partitioned/id_hash=0"));
    Assert.assertEquals("Should accept a full sub-partition Path",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have a correct relative Path",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        URI.create("/tmp/datasets/partitioned/id_hash=0"));
    Assert.assertEquals("Should accept a schemeless URI",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have a correct relative URI",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        new Path("/tmp/datasets/partitioned/id_hash=0"));
    Assert.assertEquals("Should accept a schemeless Path",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have a correct relative Path",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        URI.create("file:/tmp/datasets/partitioned/"));
    Assert.assertEquals("Should strip trailing slash from full URI",
        URI.create("file:/tmp/datasets/partitioned"),
        partition.getLocation());
    Assert.assertEquals("Should should strip trailing slash from relative URI",
        null, partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        URI.create("file:/tmp/datasets/partitioned/id_hash=0/"));
    Assert.assertEquals("Should strip trailing slash from full URI",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should should strip trailing slash from relative URI",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        URI.create("file:/tmp/datasets/partitioned/id_hash=5"));
    Assert.assertEquals("Should accept non-existent full URI",
        URI.create("file:/tmp/datasets/partitioned/id_hash=5"),
        partition.getLocation());
    Assert.assertEquals("Should should have correct non-existent relative URI",
        URI.create("id_hash=5"), partition.getRelativeLocation());

    TestHelpers.assertThrows("Should reject paths not in the dataset",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(
                "file:/tmp/datasets/unpartitioned"));
          }
        });

    TestHelpers.assertThrows("Should reject paths not in the dataset",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(
                "file:/tmp/datasets"));
          }
        });

    TestHelpers.assertThrows("Should reject paths in other file systems",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(
                "hdfs:/tmp/datasets/partitioned"));
          }
        });

    TestHelpers.assertThrows("Should reject paths deeper than partitions",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(
                "hdfs:/tmp/datasets/partitioned/id_hash=0/data_hash=2"));
          }
        });

    TestHelpers.assertThrows("Should reject invalid paths",
        NumberFormatException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(
                "file:/tmp/datasets/partitioned/id_hash=trees"));
          }
        });
  }

  @Test
  public void testRelativePaths() {
    FileSystemPartitionView<TestRecord> partition = partitioned
        .getPartitionView(URI.create("id_hash=0"));
    Assert.assertEquals("Should accept a relative URI",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have a the same relative URI",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView(
        new Path("id_hash=0"));
    Assert.assertEquals("Should accept a relative Path",
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"),
        partition.getLocation());
    Assert.assertEquals("Should have the an equivalent relative Path",
        URI.create("id_hash=0"), partition.getRelativeLocation());

    partition = partitioned.getPartitionView((URI) null);
    Assert.assertEquals("Should accept a null URI",
        URI.create("file:/tmp/datasets/partitioned"),
        partition.getLocation());
    Assert.assertEquals("Should have the an equivalent relative URI",
        null, partition.getRelativeLocation());

    partition = partitioned.getPartitionView((Path) null);
    Assert.assertEquals("Should accept a null Path",
        URI.create("file:/tmp/datasets/partitioned"),
        partition.getLocation());
    Assert.assertEquals("Should have the an equivalent relative Path",
        null, partition.getRelativeLocation());

    TestHelpers.assertThrows("Should reject empty Path",
        NumberFormatException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.getPartitionView(URI.create(""));
          }
        });
  }

  @Test
  public void testCoveringPartitions() {
    Iterable<PartitionView<TestRecord>> partitions = unpartitioned
        .getCoveringPartitions();
    Assert.assertEquals("Should have a single partition view at the root",
        unpartitioned.getPartitionView(URI.create(
            "file:/tmp/datasets/unpartitioned")),
        Iterables.getOnlyElement(partitions));

    partitions = partitioned.getCoveringPartitions();
    Set<PartitionView<TestRecord>> expected = Sets.newHashSet();
    expected.add(partitioned.getPartitionView(URI.create(
        "file:/tmp/datasets/partitioned/id_hash=0")));
    expected.add(partitioned.getPartitionView(new Path(
        "file:/tmp/datasets/partitioned/id_hash=1")));
    expected.add(partitioned.getPartitionView(URI.create(
        "file:/tmp/datasets/partitioned/id_hash=2")));
    expected.add(partitioned.getPartitionView(new Path(
        "file:/tmp/datasets/partitioned/id_hash=3")));
    Assert.assertEquals("Should have a partition view for each partition",
        expected, Sets.newHashSet(partitions));

    PartitionView<TestRecord> partition0 = partitioned.getPartitionView(
        URI.create("file:/tmp/datasets/partitioned/id_hash=0"));
    partition0.deleteAll();
    expected.remove(partition0);
    Assert.assertEquals("Should have a partition view for each partition",
        expected, Sets.newHashSet(partitions));
  }

  @Test
  public void testDeletePartitions() {
    Iterable<PartitionView<TestRecord>> partitions = partitioned
        .getCoveringPartitions();

    Set<PartitionView<TestRecord>> expected = Sets.newHashSet(partitions);
    for (PartitionView<TestRecord> partition : partitions) {
      Assert.assertTrue("Should delete data", partition.deleteAll());

      expected.remove(partition);
      Set<PartitionView<TestRecord>> actual = Sets.newHashSet(
          partitioned.getCoveringPartitions());
      Assert.assertEquals("Should only list remaining partitions",
          expected, actual);
    }

    for (PartitionView<TestRecord> partition : partitions) {
      Assert.assertFalse("Should indicate no data was present",
          partition.deleteAll());
    }
  }

  @Test
  public void testRestrictedRead() throws IOException {
    FileSystemPartitionView<TestRecord> partition0 = partitioned
        .getPartitionView(URI.create("id_hash=0"));
    FileSystemPartitionView<TestRecord> partition1 = partitioned
        .getPartitionView(URI.create("id_hash=1"));
    FileSystemPartitionView<TestRecord> partition2 = partitioned
        .getPartitionView(URI.create("id_hash=2"));
    FileSystemPartitionView<TestRecord> partition3 = partitioned
        .getPartitionView(URI.create("id_hash=3"));

    int count0 = DatasetTestUtilities.materialize(partition0).size();
    int total = DatasetTestUtilities.materialize(partitioned).size();
    Assert.assertTrue("Should read some records", count0 > 0);
    Assert.assertTrue("Should not read the entire dataset", count0 < total);

    // move other partitions so they match the partition0 constraint
    FileSystem local = LocalFileSystem.getInstance();
    local.rename(
        new Path(partition1.getLocation()),
        new Path(partitioned.getDirectory(), "0"));
    local.rename(
        new Path(partition2.getLocation()),
        new Path(partitioned.getDirectory(), "hash=0"));
    local.rename(
        new Path(partition3.getLocation()),
        new Path(partitioned.getDirectory(), "id_hash=00"));

    int newCount0 = DatasetTestUtilities.materialize(partition0).size();
    Assert.assertEquals("Should match original count", count0, newCount0);

    int countByConstraints = DatasetTestUtilities
        .materialize(partition0.toConstraintsView()).size();
    Assert.assertEquals("Should match total count", total, countByConstraints);
  }

  @Test
  public void testRestrictedDelete() throws IOException {
    FileSystemPartitionView<TestRecord> partition0 = partitioned
        .getPartitionView(URI.create("id_hash=0"));
    FileSystemPartitionView<TestRecord> partition1 = partitioned
        .getPartitionView(URI.create("id_hash=1"));
    FileSystemPartitionView<TestRecord> partition2 = partitioned
        .getPartitionView(URI.create("id_hash=2"));
    FileSystemPartitionView<TestRecord> partition3 = partitioned
        .getPartitionView(URI.create("id_hash=3"));

    int count0 = DatasetTestUtilities.materialize(partition0).size();
    int total = DatasetTestUtilities.materialize(partitioned).size();
    Assert.assertTrue("Should read some records", count0 > 0);
    Assert.assertTrue("Should not read the entire dataset", count0 < total);

    // move other partitions so they match the partition0 constraint
    FileSystem local = LocalFileSystem.getInstance();
    local.rename(
        new Path(partition1.getLocation()),
        new Path(partitioned.getDirectory(), "0"));
    local.rename(
        new Path(partition2.getLocation()),
        new Path(partitioned.getDirectory(), "hash=0"));
    local.rename(
        new Path(partition3.getLocation()),
        new Path(partitioned.getDirectory(), "id_hash=00"));

    Assert.assertEquals("Constraints should match all 4 directories", total,
        DatasetTestUtilities.materialize(partition0.toConstraintsView()).size());

    partition0.deleteAll();

    int newCount0 = DatasetTestUtilities.materialize(partition0).size();
    Assert.assertEquals("Should have removed all records in id_hash=0",
        0, newCount0);

    Assert.assertTrue("Should not have deleted other directories",
        local.exists(new Path(partitioned.getDirectory(), "0")));
    Assert.assertTrue("Should not have deleted other directories",
        local.exists(new Path(partitioned.getDirectory(), "hash=0")));
    Assert.assertTrue("Should not have deleted other directories",
        local.exists(new Path(partitioned.getDirectory(), "id_hash=00")));

    Assert.assertEquals("Should match total without deleted data",
        total - count0,
        DatasetTestUtilities.materialize(partition0.toConstraintsView()).size());

    partitioned.unbounded.deleteAll();

    Assert.assertFalse("Should have deleted all other directories",
        local.exists(new Path(partitioned.getDirectory(), "0")));
    Assert.assertFalse("Should have deleted all other directories",
        local.exists(new Path(partitioned.getDirectory(), "hash=0")));
    Assert.assertFalse("Should have deleted all other directories",
        local.exists(new Path(partitioned.getDirectory(), "id_hash=00")));
  }

  private static void writeTestRecords(View<TestRecord> view) {
    DatasetWriter<TestRecord> writer = null;
    try {
      writer = view.newWriter();
      for (int i = 0; i < 10; i += 1) {
        TestRecord record = new TestRecord();
        record.id = i;
        record.data = "test-" + i;
        writer.write(record);
      }

    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }
}
