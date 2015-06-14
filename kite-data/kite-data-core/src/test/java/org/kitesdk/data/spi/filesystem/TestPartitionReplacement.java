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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
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
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.View;

public class TestPartitionReplacement {
  public static class TestRecord {
    private long id;
    private String data;
  }

  private FileSystemDataset<TestRecord> unpartitioned = null;
  private FileSystemDataset<TestRecord> partitioned = null;
  private FileSystemDataset<TestRecord> temporary = null;

  @Before
  public void createTestDatasets() {
    Datasets.delete("dataset:file:/tmp/datasets/unpartitioned");
    Datasets.delete("dataset:file:/tmp/datasets/partitioned");
    Datasets.delete("dataset:file:/tmp/datasets/temporary");

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(TestRecord.class)
        .build();
    unpartitioned = Datasets.create("dataset:file:/tmp/datasets/unpartitioned",
        descriptor, TestRecord.class);

    descriptor = new DatasetDescriptor.Builder(descriptor)
        .property("kite.writer.cache-size", "20")
        .partitionStrategy(new PartitionStrategy.Builder()
            .hash("id", 4)
            .build())
        .build();
    partitioned = Datasets.create("dataset:file:/tmp/datasets/partitioned",
        descriptor, TestRecord.class);

    // create a second dataset with the same partitioning for replacement parts
    temporary = Datasets.create("dataset:file:/tmp/datasets/temporary",
        descriptor, TestRecord.class);

    writeTestRecords(unpartitioned);
    writeTestRecords(partitioned);
    writeTestRecords(temporary);
  }

  @After
  public void removeTestDatasets() {
    Datasets.delete("dataset:file:/tmp/datasets/unpartitioned");
    Datasets.delete("dataset:file:/tmp/datasets/partitioned");
    Datasets.delete("dataset:file:/tmp/datasets/temporary");
  }

  @Test
  public void testUnpartitionedReplace() {
    // recreate temporary without a partition strategy
    Datasets.delete("dataset:file:/tmp/datasets/temporary");
    DatasetDescriptor descriptor = new DatasetDescriptor
        .Builder(unpartitioned.getDescriptor())
        .location((URI) null) // clear the location
        .build();
    temporary = Datasets.create("dataset:file:/tmp/datasets/temporary",
        descriptor, TestRecord.class);

    Assert.assertTrue("Should allow replacing an unpartitioned dataset",
        unpartitioned.canReplace(unpartitioned));

    // make sure there are multiple files
    writeTestRecords(unpartitioned);
    writeTestRecords(unpartitioned);
    writeTestRecords(temporary);
    writeTestRecords(temporary);

    Set<String> originalFiles = Sets.newHashSet(
        Iterators.transform(unpartitioned.pathIterator(), new GetFilename()));
    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temporary.pathIterator(), new GetFilename()));

    Iterators.transform(temporary.pathIterator(), new GetFilename());
    Assert.assertFalse("Sanity check", originalFiles.equals(replacementFiles));

    unpartitioned.replace(unpartitioned, temporary);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(unpartitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the replacement files",
        replacementFiles, replacedFiles);
  }

  @Test
  public void testUnpartitionedReplaceDifferentStrategy() {
    final FileSystemPartitionView<TestRecord> partition0 = partitioned
        .getPartitionView(new Path("id_hash=0"));

    Assert.assertFalse("Should not allow replacement with a different strategy",
        unpartitioned.canReplace(partitioned));
    Assert.assertFalse("Should not allow replacement with a different strategy",
        unpartitioned.canReplace(partition0));

    TestHelpers.assertThrows(
        "Should not allow replacement with a different partition strategy",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            unpartitioned.replace(unpartitioned, partitioned);
          }
        });
    TestHelpers.assertThrows(
        "Should not allow replacement with a different partition strategy",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            unpartitioned.replace(unpartitioned, partition0);
          }
        });
  }

  @Test
  public void testReplaceWithDifferentStrategy() {
    Assert.assertFalse("Should not allow replacement with a different strategy",
        partitioned.canReplace(unpartitioned));

    TestHelpers.assertThrows(
        "Should not allow replacement with a different partition strategy",
        ValidationException.class, new Runnable() {
          @Override
          public void run() {
            partitioned.replace(partitioned, unpartitioned);
          }
        });
  }

  @Test
  public void testPartitionedReplace() {
    Assert.assertTrue("Should allow replacing a whole dataset",
        partitioned.canReplace(partitioned));
    Assert.assertTrue(
        "Should not allow replacement test with a different dataset",
        !partitioned.canReplace(temporary));

    Set<String> originalFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temporary.pathIterator(), new GetFilename()));
    Assert.assertEquals("Sanity check",
        originalFiles.size(), replacementFiles.size());
    Assert.assertFalse("Sanity check", originalFiles.equals(replacementFiles));

    partitioned.replace(partitioned, temporary);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the replacement files",
        replacementFiles, replacedFiles);
  }

  @Test
  public void testReplaceSinglePartition() {
    FileSystemPartitionView<TestRecord> partition0 = partitioned.getPartitionView(
        new Path("id_hash=0"));
    FileSystemPartitionView<TestRecord> temp0 = temporary.getPartitionView(
        new Path("id_hash=0"));

    Assert.assertTrue("Should allow replacing a single partition",
        partitioned.canReplace(partition0));
    Assert.assertFalse(
        "Should not allow replacement test with a different dataset",
        partitioned.canReplace(temp0));

    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temp0.pathIterator(), new GetFilename()));
    Set<String> originalPartitionFiles = Sets.newHashSet(
        Iterators.transform(partition0.pathIterator(), new GetFilename()));

    Assert.assertEquals("Sanity check",
        originalPartitionFiles.size(), replacementFiles.size());
    Assert.assertFalse("Sanity check",
        originalPartitionFiles.equals(replacementFiles));

    Set<String> expectedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    expectedFiles.removeAll(originalPartitionFiles);
    expectedFiles.addAll(replacementFiles);

    partitioned.replace(partition0, temp0);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the replacement files",
        expectedFiles, replacedFiles);
  }

  @Test
  public void testReplacePartitionsByConstraints() throws IOException {
    // like testReplaceSinglePartition, this will replace partition0 with temp0
    // but, this will also remove partitions that have equivalent constraints
    // to simulate the case where directories 0, hash_0, id_hash=0, and
    // id_hash=00 are compacted to a single replacement folder

    FileSystemPartitionView<TestRecord> partition0 = partitioned.getPartitionView(
        new Path("id_hash=0"));
    FileSystemPartitionView<TestRecord> temp0 = temporary.getPartitionView(
        new Path("id_hash=0"));

    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temp0.pathIterator(), new GetFilename()));

    // move other partitions so they match the partition0 constraint
    FileSystem local = LocalFileSystem.getInstance();
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=1"),
        new Path(partitioned.getDirectory(), "0"));
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=2"),
        new Path(partitioned.getDirectory(), "hash=0"));
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=3"),
        new Path(partitioned.getDirectory(), "id_hash=00"));

    Assert.assertTrue("Should allow replacing a single partition",
        partitioned.canReplace(partition0));
    Assert.assertFalse(
        "Should not allow replacement test with a different dataset",
        partitioned.canReplace(temp0));

    partitioned.replace(partition0, temp0);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the replacement files",
        replacementFiles, replacedFiles);

    Iterator<Path> dirIterator = partitioned.dirIterator();
    Path onlyDirectory = dirIterator.next();
    Assert.assertFalse("Should contain only one directory",
        dirIterator.hasNext());
    Assert.assertEquals("Should have the correct directory name",
        "id_hash=0", onlyDirectory.getName());
  }

  @Test
  public void testReplacePartitionsByConstraintsWithoutOriginal() throws IOException {
    // like testReplacePartitionsByConstraints, but the target partition does
    // not exist

    FileSystemPartitionView<TestRecord> temp0 = temporary.getPartitionView(
        new Path("id_hash=0"));

    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temp0.pathIterator(), new GetFilename()));

    // move other partitions so they match the partition0 constraint
    FileSystem local = LocalFileSystem.getInstance();
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=0"),
        new Path(partitioned.getDirectory(), "id-hash=0"));
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=1"),
        new Path(partitioned.getDirectory(), "0"));
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=2"),
        new Path(partitioned.getDirectory(), "hash=0"));
    local.rename(
        new Path(partitioned.getDirectory(), "id_hash=3"),
        new Path(partitioned.getDirectory(), "id_hash=00"));

    FileSystemPartitionView<TestRecord> partition0 = partitioned.getPartitionView(
        new Path("id-hash=0"));

    Assert.assertTrue("Should allow replacing a single partition",
        partitioned.canReplace(partition0));
    Assert.assertFalse(
        "Should not allow replacement test with a different dataset",
        partitioned.canReplace(temp0));

    partitioned.replace(partition0, temp0);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the replacement files",
        replacementFiles, replacedFiles);

    Iterator<Path> dirIterator = partitioned.dirIterator();
    Path onlyDirectory = dirIterator.next();
    Assert.assertFalse("Should contain only one directory",
        dirIterator.hasNext());
    Assert.assertEquals("Should have the correct directory name",
        "id_hash=0", onlyDirectory.getName());
  }

  @Test
  public void testReplaceRemovesPartitionsNotIndividuallyReplaced() throws IOException {
    // remove 2 of the partitions in the temp dataset and replace the entire
    // partitioned dataset. the partitions that weren't replaced in the final
    // dataset should not exist either.

    Assert.assertTrue("Should allow replacing a whole dataset",
        partitioned.canReplace(partitioned));
    Assert.assertTrue(
        "Should not allow replacement test with a different dataset",
        !partitioned.canReplace(temporary));

    // delete the odd partition numbers
    FileSystem local = LocalFileSystem.getInstance();
    local.delete(
        new Path(temporary.getDirectory(), "id_hash=1"),
        true /* recursive */);
    local.delete(
        new Path(temporary.getDirectory(), "id_hash=3"),
        true /* recursive */);
    // also delete a partition that will be replaced in the partitioned dataset
    local.delete(
        new Path(partitioned.getDirectory(), "id_hash=2"),
        true /* recursive */);

    Set<String> originalFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Set<String> replacementFiles = Sets.newHashSet(
        Iterators.transform(temporary.pathIterator(), new GetFilename()));
    Assert.assertFalse("Sanity check", originalFiles.equals(replacementFiles));

    partitioned.replace(partitioned, temporary);

    Set<String> replacedFiles = Sets.newHashSet(
        Iterators.transform(partitioned.pathIterator(), new GetFilename()));
    Assert.assertEquals("Should contain the only the replacement files",
        replacementFiles, replacedFiles);
    Assert.assertEquals("Should have only 2 files (1 in each partition)",
        2, replacedFiles.size());
  }

  private static class GetFilename implements Function<Path, String> {
    @Override
    public String apply(Path path) {
      return path.getName();
    }
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
