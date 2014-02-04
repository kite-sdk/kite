/*
 * Copyright "2013" Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.filesystem;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.StorageKey;
import org.kitesdk.data.spi.MarkerComparator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestFileSystemPartitionIterator extends MiniDFSTest {

  public FileSystem fileSystem;
  public Path testDirectory;
  public static PartitionStrategy strategy;
  public static MarkerComparator comparator;
  public static List<StorageKey> keys;

  @BeforeClass
  public static void createExpectedKeys() {
    strategy = new PartitionStrategy.Builder()
        .year("timestamp")
        .month("timestamp")
        .day("timestamp")
        .build();

    comparator = new MarkerComparator(strategy);

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

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    MiniDFSTest.setupFS();
    Object[][] data = new Object[][] {
        { getDFS() },
        { getFS() } };
    return Arrays.asList(data);
  }

  public TestFileSystemPartitionIterator(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
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
        fileSystem, testDirectory, strategy, new Constraints());

    assertIterableEquals(keys, partitions);
  }

  public static final long oct_25_2012 = new DateTime(2012, 10, 25, 0, 0, DateTimeZone.UTC).getMillis();
  public static final long oct_24_2013 = new DateTime(2013, 10, 24, 0, 0, DateTimeZone.UTC).getMillis();
  public static final long oct_25_2013 = new DateTime(2013, 10, 25, 0, 0, DateTimeZone.UTC).getMillis();
  public static final long oct_24_2013_end = oct_25_2013 - 1;

  @Test
  public void testFrom() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().from("timestamp", oct_24_2013));
    assertIterableEquals(keys.subList(16, 24), partitions);
  }

  @Test
  public void testAfter() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().fromAfter("timestamp", oct_24_2013_end));
    assertIterableEquals(keys.subList(17, 24), partitions);
  }

  @Test
  public void testTo() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().to("timestamp", oct_25_2012));
    assertIterableEquals(keys.subList(0, 6), partitions);
  }

  @Test
  public void testBefore() throws Exception {
    Iterable <StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().toBefore("timestamp", oct_25_2012));
    assertIterableEquals(keys.subList(0, 5), partitions);
  }

  @Test
  public void testWith() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().with("timestamp", oct_24_2013));
    assertIterableEquals(keys.subList(16, 17), partitions);
  }

  @Test
  public void testDayRange() throws Exception {
    Iterable<StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().from("timestamp", oct_24_2013).to("timestamp", oct_24_2013_end));
    assertIterableEquals(keys.subList(16, 17), partitions);
  }

  @Test
  public void testLargerRange() throws Exception {
    Iterable <StorageKey> partitions = new FileSystemPartitionIterator(
        fileSystem, testDirectory, strategy,
        new Constraints().from("timestamp", oct_25_2012).to("timestamp", oct_24_2013));
    assertIterableEquals(keys.subList(5, 17), partitions);
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
