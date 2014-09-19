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
package org.kitesdk.minicluster.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.minicluster.MiniCluster;
import org.slf4j.Logger;

@Parameters(commandDescription = "Run a minicluster")
public class RunCommand implements Command {

  private final Logger console;
  private final Configuration conf;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_NULL_FIELD", justification = "Field set by JCommander")
  @Parameter(names = { "-d", "--directory" }, description = "The base directory to store mini cluster data in.")
  String localBaseFsLocation = null;

  @Parameter(names = { "-z", "--zkport" }, description = "The port zookeeper should be listening on.")
  int zkClientPort = 28282;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UWF_NULL_FIELD", justification = "Field set by JCommander")
  @Parameter(names = { "-b", "--bind" }, description = "The IP address for all mini cluster services to bind to.")
  String forceBindIP = null;

  @Parameter(names = "--clean", description = "Clean the mini cluster data directory before starting.")
  boolean clean = false;

  @Parameter(names = "--hdfs", description = "Run an HDFS service in the mini cluster.")
  boolean hdfs = false;

  @Parameter(names = "--hbase", description = "Run an HBase service in the mini cluster.")
  boolean hbase = false;

  @Parameter(names = "--zk", description = "Run a Zookeeper service in the mini cluster.")
  boolean zk = false;

  public RunCommand(Logger console, Configuration conf) {
    this.console = console;
    this.conf = conf;
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(hdfs || hbase || zk,
        "At least one service must be specified.");
    Preconditions
        .checkArgument(localBaseFsLocation != null,
            "Minicluster directory and a valid zookeeper client port are required.");

    MiniCluster.Builder builder = new MiniCluster.Builder()
        .localBaseFsLocation(localBaseFsLocation).clean(clean).conf(conf);
    if (forceBindIP != null) {
      builder.forceBindIP(forceBindIP);
    }
    if (hdfs) {
      builder.addHdfsService();
    }
    if (zk) {
      builder.addZookeeperService(zkClientPort);
    }
    if (hbase) {
      builder.addHBaseService();
    }
    final MiniCluster miniCluster = builder.build();

    // Create an exit thread that listens for a kill command, and notifies the
    // main thread to exit.
    final Object wait = new Object();
    Thread exitThread = new Thread() {
      @Override
      public void run() {
        try {
          miniCluster.stop();
        } catch (Throwable e) {
          console.error("Error stopping mini cluster. Exiting anyways...", e);
        }
        wait.notify();
      }
    };
    Runtime.getRuntime().addShutdownHook(exitThread);

    // Start the mini cluster, and wait for the exit notification.
    miniCluster.start();
    try {
      synchronized(wait) {
        wait.wait();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists
        .newArrayList(
            "# Run a mini HDFS cluster:",
            "--hdfs -d /tmp/kite-minicluster",
            "# Run an HBase cluster that forces everything to listen on IP 10.0.0.1:",
            "--hdfs --zk --hbase -d /tmp/kite-minicluster -b 10.0.0.1",
            "# Run an HBase cluster, cleaning out any data from previous cluster runs:",
            "--hdfs --zk --hbase -d /tmp/kite-minicluster --clean");
  }

}
