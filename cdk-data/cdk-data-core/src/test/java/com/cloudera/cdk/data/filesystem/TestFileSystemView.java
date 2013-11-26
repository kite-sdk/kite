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

package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.TestRangeViews;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.event.StandardEvent;
import com.cloudera.cdk.data.spi.Key;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

public class TestFileSystemView extends TestRangeViews {

  public TestFileSystemView(boolean distributed) {
    super(distributed);
  }

  @Override
  public DatasetRepository newRepo() {
    return new FileSystemDatasetRepository.Builder()
        .configuration(conf)
        .rootDirectory(URI.create("target/data"))
        .build();
  }

  @After
  public void removeDataPath() throws IOException {
    fs.delete(new Path("target/data"), true);
  }

  @Test
  @Override
  @SuppressWarnings("unchecked")
  public void testCoveringPartitions() {
    // NOTE: this is an un-restricted write so all should succeed
    final DatasetWriter<StandardEvent> writer = testDataset.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

    final Set<View<StandardEvent>> expected = Sets.newHashSet(
        testDataset.in(new Key(strategy, sepEvent)),
        testDataset.in(new Key(strategy, octEvent)),
        testDataset.in(new Key(strategy, novEvent)));

    Assert.assertEquals("Covering partitions should match",
        expected, Sets.newHashSet(testDataset.getCoveringPartitions()));
  }

  @Test
  public void testDelete() throws Exception {
    // NOTE: this is an un-restricted write so all should succeed
    final DatasetWriter<StandardEvent> writer = testDataset.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

    final Path root = new Path("target/data/test");
    final Path y2013 = new Path("target/data/test/year=2013");
    final Path sep = new Path("target/data/test/year=2013/month=09");
    final Path sep12 = new Path("target/data/test/year=2013/month=09/day=12");
    final Path oct = new Path("target/data/test/year=2013/month=10");
    final Path oct12 = new Path("target/data/test/year=2013/month=10/day=12");
    final Path nov = new Path("target/data/test/year=2013/month=11");
    final Path nov11 = new Path("target/data/test/year=2013/month=11/day=11");
    assertDirectoriesExist(fs, root, y2013, sep, sep12, oct, oct12, nov, nov11);

    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.from(newMarker(2013, 6)).toBefore(newMarker(2013, 9))
            .deleteAll());
    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.from(newMarker(2013, 11, 12)).deleteAll());

    // delete everything up to September
    Assert.assertTrue("Delete should return true to indicate FS changed",
        testDataset.to(newMarker(2013, 9)).deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep);
    assertDirectoriesExist(fs, root, y2013, oct, oct12, nov, nov11);
    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.to(newMarker(2013, 9)).deleteAll());

    // delete November 11 and later
    Assert.assertTrue("Delete should return true to indicate FS changed",
        testDataset.from(newMarker(2013, 11, 11)).to(newMarker(2013, 11, 12))
            .deleteAll());
    assertDirectoriesDoNotExist(fs, sep12, sep, nov11, nov);
    assertDirectoriesExist(fs, root, y2013, oct, oct12);
    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.from(newMarker(2013, 11, 11)).to(newMarker(2013, 11, 12))
            .deleteAll());

    // delete October and the 2013 directory
    Assert.assertTrue("Delete should return true to indicate FS changed",
        testDataset.in(newMarker(2013, 10, 12)).deleteAll());
    assertDirectoriesDoNotExist(fs, y2013, sep12, sep, oct12, oct, nov11, nov);
    assertDirectoriesExist(fs, root);
    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.in(newMarker(2013, 10, 12)).deleteAll());

    Assert.assertFalse("Delete should return false to indicate no changes",
        testDataset.deleteAll());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testLimitedAccessor() {
    testDataset.newAccessor();
  }

  public static void assertDirectoriesExist(FileSystem fs, Path... dirs)
      throws IOException {
    for (Path path : dirs) {
      Assert.assertTrue("Directory should exist: " + path,
          fs.exists(path) && fs.isDirectory(path));
    }
  }

  public static void assertDirectoriesDoNotExist(FileSystem fs, Path... dirs)
      throws IOException {
    for (Path path : dirs) {
      Assert.assertTrue("Directory should not exist: " + path,
          !fs.exists(path));
    }
  }
}
