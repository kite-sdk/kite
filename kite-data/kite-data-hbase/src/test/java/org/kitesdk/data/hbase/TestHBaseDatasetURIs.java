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

package org.kitesdk.data.hbase;

import java.net.URI;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.RandomAccessDataset;
import org.kitesdk.data.TestHelpers;
import org.kitesdk.data.hbase.impl.Loader;
import org.kitesdk.data.hbase.testing.HBaseTestUtils;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;

public class TestHBaseDatasetURIs {

  private static DatasetDescriptor descriptor;
  private static String zk;
  private static URI repositoryUri;

  @BeforeClass
  public static void startHBase() throws Exception {
    new Loader().load();
    descriptor = new DatasetDescriptor.Builder()
        .schemaUri("resource:TestGenericEntity.avsc")
        .build();
    HBaseTestUtils.getMiniCluster();
    String zkQuorum = HBaseTestUtils.getConf().get(HConstants.ZOOKEEPER_QUORUM);
    String zkClientPort = HBaseTestUtils.getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    zk = zkQuorum + ":" + zkClientPort; // OK since zkQuorum is a single host
    repositoryUri = new URI("repo:hbase:" + zk);
  }

  @Test
  public void testBasic() {
    DatasetRepository repo = DatasetRepositories.repositoryFor(repositoryUri);
    repo.delete("default", "test");
    repo.create("default", "test", descriptor);

    RandomAccessDataset<Object> ds = Datasets
	.<Object, RandomAccessDataset<Object>>load(URI.create("dataset:hbase:" + zk + "/test"), Object.class);

    Assert.assertNotNull("Should load dataset", ds);
    Assert.assertTrue(ds instanceof DaoDataset);
    Assert.assertEquals("Descriptors should match",
        repo.load("default", "test").getDescriptor(), ds.getDescriptor());

    repo.delete("default", "test");
  }

  @Test
  public void testMissingDataset() {
    TestHelpers.assertThrows("Should not find dataset: no such dataset",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets
                .<Object, Dataset<Object>>load("dataset:hbase:" + zk + "/nosuchdataset", Object.class);
          }
        }
    );
  }

  @Test
  public void testMissingRepository() {
    TestHelpers.assertThrows("Should not find dataset: unknown storage scheme",
        DatasetNotFoundException.class, new Runnable() {
          @Override
          public void run() {
            Dataset<Object> ds = Datasets
                .<Object, Dataset<Object>>load("dataset:unknown:" + zk + "/test", Object.class);
          }
        });
  }
}
