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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.LocalFileSystem;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.StorageKey;

public class TestFileSystemPartitionIteratorProvidedPartitioners {

  public static final PartitionStrategy strategy = new PartitionStrategy.Builder()
      .provided("year")
      .provided("month", "int")
      .provided("day", "long")
      .build();

  private static final Schema schema = SchemaBuilder.record("Event").fields()
      .requiredLong("id")
      .requiredLong("timestamp")
      .endRecord();

  public static final Constraints emptyConstraints = new Constraints(schema, strategy);

  public static FileSystem fileSystem;
  public static List<StorageKey> keys;

  public Path testDirectory;
  @BeforeClass
  public static void createExpectedKeys() throws IOException {
    fileSystem = LocalFileSystem.getInstance();
    keys = Lists.newArrayList();
    for (Object year : Arrays.asList(2012, 2013)) {
      for (Object month : Arrays.asList(9, 10, 11, 12)) {
        for (Object day : Arrays.asList(22, 24, 25)) {
          StorageKey k = new StorageKey.Builder(strategy)
              .add("year", year).add("month", month).add("day", day).build();
          keys.add(k);
        }
      }
    }
  }

  @Before
  public void createDirectoryLayout() throws Exception {
    testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));

    for (String year : Arrays.asList("year=2012", "year=2013")) {
      final Path yearPath = new Path(testDirectory, year);
      for (String month : Arrays.asList("month=09", "month=10", "month=11", "month=12")) {
        final Path monthPath = new Path(yearPath, month);
        for (String day : Arrays.asList("day=22", "day=24", "day=25")) {
          final Path dayPath = new Path(monthPath, day);
          fileSystem.mkdirs(dayPath);
        }
      }
    }
  }

  @After
  public void cleanDirectoryLayout() throws Exception {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testUnbounded() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy, schema, emptyConstraints.toKeyPredicate());

    assertIterableEquals(keys, partitions);
  }

  @Test
  public void testStringConstraint() throws IOException {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy, schema,
        emptyConstraints.with("year", "2012").toKeyPredicate());
    assertIterableEquals(keys.subList(0, 12), partitions);

    TestHelpers.assertThrows("Should reject constraint with integer type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("year", 2013);
          }
        });
    TestHelpers.assertThrows("Should reject constraint with long type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("year", 2013L);
          }
        });
  }

  @Test
  public void testIntConstraint() throws IOException {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy, schema,
        emptyConstraints.with("month", 9).toKeyPredicate());
    List<StorageKey> expected = Lists.newArrayList();
    expected.addAll(keys.subList(0, 3));
    expected.addAll(keys.subList(12, 15));
    assertIterableEquals(expected, partitions);

    TestHelpers.assertThrows("Should reject constraint with string type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("month", "10");
          }
        });
    TestHelpers.assertThrows("Should reject constraint with long type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("month", 11L);
          }
        });
  }

  @Test
  public void testLongConstraint() throws IOException {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy, schema,
        emptyConstraints.with("day", 22L).toKeyPredicate());
    List<StorageKey> expected = Lists.newArrayList(
        keys.get(0), keys.get(3), keys.get(6), keys.get(9),
        keys.get(12), keys.get(15), keys.get(18), keys.get(21));
    assertIterableEquals(expected, partitions);

    TestHelpers.assertThrows("Should reject constraint with string type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("day", "24");
          }
        });
    TestHelpers.assertThrows("Should reject constraint with long type",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            emptyConstraints.with("day", 25);
          }
        });
  }

  public static <T> void assertIterableEquals(
      Iterable<T> expected, Iterable<T> actualIterable) {
    Set<T> expectedSet = Sets.newHashSet(expected);
    for (T actual : actualIterable) {
      // need to check as iteration happens because the StorageKey is reused
      Assert.assertTrue("Unexpected record: " + actual,
          expectedSet.remove(actual));
    }
    Assert.assertEquals("Not all expected records were present: " + expectedSet,
        0, expectedSet.size());
  }
}
