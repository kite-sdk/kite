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

package org.kitesdk.cli.commands;

import com.beust.jcommander.internal.Lists;
import java.io.IOException;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.TestHelpers;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

/**
 * This tests that the base command correctly builds a repository URI from the
 * given options.
 */
public class TestBaseCommand {

  public static class TestCommand extends BaseDatasetCommand {
    public TestCommand(Logger console) {
      super(console);
    }

    @Override
    public int run() throws IOException {
      return 0;
    }

    @Override
    public List<String> getExamples() {
      return null;
    }
  }

  private Logger console = null;
  private BaseDatasetCommand command = null;

  @Before
  public void createCommand() {
    this.console = mock(Logger.class);
    this.command = new TestCommand(console);
  }

  @Test
  public void testDefaults() {
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    verify(console).trace(contains("repo:hive"));
  }

  @Test
  public void testManagedHiveRepo() {
    command.hive = true;
    command.directory = null;
    Assert.assertEquals("repo:hive", command.buildRepoURI());
    verify(console).trace(contains("repo:hive"));
  }

  @Test
  public void testExternalHiveRepo() {
    command.hive = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hive:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:hive:/tmp/data"));
  }

  @Test
  public void testRelativeExternalHiveRepo() {
    command.hive = true;
    command.directory = "data";
    Assert.assertEquals("repo:hive:data", command.buildRepoURI());
    verify(console).trace(contains("repo:hive:data"));
  }

  @Test
  public void testHDFSRepo() {
    command.hdfs = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:hdfs:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:hdfs:/tmp/data"));
  }

  @Test
  public void testHDFSRepoRejectsNullPath() {
    command.hdfs = true;
    command.directory = null;
    TestHelpers.assertThrows(
        "Should reject null directory for HDFS",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        }
    );
    verifyZeroInteractions(console);
  }

  @Test
  public void testLocalRepo() {
    command.local = true;
    command.directory = "/tmp/data";
    Assert.assertEquals("repo:file:/tmp/data", command.buildRepoURI());
    verify(console).trace(contains("repo:file:/tmp/data"));
  }

  @Test
  public void testLocalDataset() {
    command.local = true;
    command.directory = "/tmp/data";
    command.namespace = "ns";
    Assert.assertEquals("dataset:file:/tmp/data/ns/users", command.buildDatasetUri("users"));
    verify(console).trace(contains("repo:file:/tmp/data"));
  }

  @Test
  public void testLocalRepoRejectsNullPath() {
    command.hive = false;
    command.local = true;
    command.directory = null;
    TestHelpers.assertThrows(
        "Should reject null directory for local",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        }
    );
    verifyZeroInteractions(console);
  }

  @Test
  public void testHBaseRepo() {
    command.hbase = true;
    command.zookeeper = Lists.newArrayList("zk1:1234", "zk2");
    Assert.assertEquals("repo:hbase:zk1:1234,zk2", command.buildRepoURI());
    verify(console).trace(contains("repo:hbase:zk1:1234,zk2"));
  }

  @Test
  public void testHbaseRepoRejectsNullZooKeeper() {
    command.hive = false;
    command.local = true;
    command.directory = null;
    TestHelpers.assertThrows(
        "Should reject null ZooKeeper for local, non-Hive",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    verifyZeroInteractions(console);
  }

  @Test
  public void testRejectsMultipleStorageSchemes() {
    command.hive = true;
    command.local = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: Hive and local",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    command.local = false;
    command.hdfs = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: Hive and HDFS",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    command.hdfs = false;
    command.hbase = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: Hive and HBase",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    command.hive = false;
    command.local = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: HBase and local",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    command.local = false;
    command.hdfs = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: HBase and HDFS",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
    command.hbase = false;
    command.local = true;
    TestHelpers.assertThrows(
        "Should reject multiple storage: HDFS and local",
        IllegalArgumentException.class,
        new Runnable() {
          @Override
          public void run() {
            command.buildRepoURI();
          }
        });
  }

}
