/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data.hbase;

import java.net.URI;
import org.kitesdk.data.spi.RandomAccessDatasetRepository;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.hbase.impl.Loader;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;

import org.apache.hadoop.hbase.HConstants;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DatasetRepositories;

public class TestHBaseRepositoryURIs {

  @BeforeClass
  public static void registerURIs() throws Exception {
    new Loader().load();
    HBaseTestUtils.getMiniCluster();
  }

  @Test
  public void testParseValidHostsAndPort() {
    checkParse("localhost", "localhost", null);
    checkParse("localhost:2000", "localhost", "2000");
    checkParse("zk1.example.org", "zk1.example.org", null);
    checkParse("zk1.example.org:2000", "zk1.example.org", "2000");
    checkParse("zk1.example.org,zk2.example.org", "zk1.example.org,zk2.example.org", null);
    checkParse("zk1.example.org:2000,zk2.example.org:2000", "zk1.example.org,zk2.example.org", "2000");
    checkParse("zk1.example.org,zk2.example.org:2000", "zk1.example.org,zk2.example.org", "2000");
    checkParse("zk1.example.org:2000,zk2.example.org", "zk1.example.org," +
        "zk2.example.org", "2000");
  }

  private void checkParse(String zkQuorum, String expectedHosts, String expectedPort) {
    String[] hostsAndPorts = Loader.parseHostsAndPort(zkQuorum);
    assertEquals(expectedHosts, hostsAndPorts[0]);
    assertEquals(expectedPort, hostsAndPorts[1]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseMismatchedPorts() {
    Loader.parseHostsAndPort("zk1.example.org:2000,zk2.example.org:2001");
  }

  @Test
  public void testHBaseURI() throws Exception {
    String zkQuorum = HBaseTestUtils.getConf().get(HConstants.ZOOKEEPER_QUORUM);
    String zkClientPort = HBaseTestUtils.getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    String zk = zkQuorum + ":" + zkClientPort; // OK since zkQuorum is a single host
    URI repositoryUri = new URI("repo:hbase:" + zk);
    RandomAccessDatasetRepository repo = DatasetRepositories.repositoryFor(repositoryUri);

    Assert.assertNotNull("Received a repository", repo);
    assertTrue("Repo is a HBase repo", repo instanceof HBaseDatasetRepository);
    assertEquals("Repository URI", repositoryUri, repo.getUri());
  }

  @Test
  public void testPathFailsMatch() {
    TestHelpers.assertThrows("Should not match URIs that contain '/'",
        IllegalArgumentException.class, new Runnable() {
      @Override
      public void run() {
        DatasetRepositories.repositoryFor("repo:hbase:zk1,zk2:2000/path");
      }
    });
  }

}
