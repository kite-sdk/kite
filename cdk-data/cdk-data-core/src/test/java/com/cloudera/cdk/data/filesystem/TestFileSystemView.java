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
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

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
    // TODO: finish this test. requires View#equals implementation
    List<View<StandardEvent>> partitions = Lists.newArrayList(
        testDataset.in(new Key(strategy).reuseFor(sepEvent)),
        testDataset.in(new Key(strategy).reuseFor(octEvent)),
        testDataset.in(new Key(strategy).reuseFor(novEvent)));
    DatasetWriter<StandardEvent> writer = testDataset.newWriter();
    try {
      writer.open();
      writer.write(sepEvent);
      writer.write(octEvent);
      writer.write(novEvent);
    } finally {
      writer.close();
    }

//    Assert.assertEquals(partitions, testDataset.getCoveringPartitions());
//    Assert.assertEquals(partitions,
//        testDataset.in(newMarker(2013)).getCoveringPartitions());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testLimitedAccessor() {
    testDataset.newAccessor();
  }
}
