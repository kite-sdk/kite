/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.minicluster;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

public class TestMiniCluster {

  @Test(expected = IllegalStateException.class)
  public void testNoLocalBaseFsLocation() {
    new MiniCluster.Builder().addService(HdfsService.class).build();
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleService() {
    new MiniCluster.Builder().workDir("/tmp/kite-minicluster")
        .addService(HdfsService.class).addService(HdfsService.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingDependency() {
    new MiniCluster.Builder().workDir("/tmp/kite-minicluster")
        .bindIP("127.0.0.1").addService(HdfsService.class)
        .addService(HBaseService.class).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingFlumeConfiguration() {
    new MiniCluster.Builder().addService(FlumeService.class)
        .flumeConfiguration("resource:missing-flume.properties").flumeAgentName("tier1")
        .build();
  }

  @Test
  public void testValidMiniCluster() throws IOException, InterruptedException {
    String workDir = "target/kite-minicluster-workdir-" + UUID.randomUUID();
    MiniCluster miniCluster = new MiniCluster.Builder().workDir(workDir)
        .bindIP("127.0.0.1")
        .addService(HdfsService.class)
        .addService(ZookeeperService.class)
        .addService(HBaseService.class)
        .addService(HiveService.class)
        .clean(true).build();
    miniCluster.start();
    miniCluster.stop();
  }
}
