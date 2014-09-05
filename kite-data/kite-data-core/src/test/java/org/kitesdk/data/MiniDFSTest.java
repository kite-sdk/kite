/*
 * Copyright 2013 Cloudera.
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
package org.kitesdk.data;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kitesdk.compat.DynMethods;

/**
 * Provides setup/teardown of a MiniDFSCluster for tests that need one.
 */
public class MiniDFSTest {
  private static Configuration conf = null;
  private static MiniDFSCluster cluster = null;
  private static FileSystem dfs = null;
  private static FileSystem lfs = null;

  private static DynMethods.UnboundMethod getFS = new DynMethods
      .Builder("getFileSystem")
      .impl(MiniDFSCluster.class)
      .build();

  protected static Configuration getConfiguration() {
    return conf;
  }

  protected static FileSystem getDFS() {
    return dfs;
  }

  protected static FileSystem getFS() {
    return lfs;
  }

  @BeforeClass
  @SuppressWarnings("deprecation")
  public static void setupFS() throws IOException {
    if (cluster == null) {
      Configuration c = new Configuration();
      c.setBoolean("dfs.webhdfs.enabled", true);
      // if this fails with "The directory is already locked" set umask to 0022
      cluster = new MiniDFSCluster(c, 1, true, null);
      //cluster = new MiniDFSCluster.Builder(new Configuration()).build();
      dfs = getFS.invoke(cluster);
      conf = new Configuration(dfs.getConf());
      lfs = FileSystem.getLocal(conf);
    }
  }

  @AfterClass
  public static void teardownFS() throws IOException {
    dfs = null;
    lfs = null;
    conf = null;
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
