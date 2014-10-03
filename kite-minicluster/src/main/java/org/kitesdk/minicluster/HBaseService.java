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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.net.DNS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HBase minicluster service implementation.
 */
public class HBaseService implements Service {

  private static final Logger logger = LoggerFactory
      .getLogger(HBaseService.class);

  /**
   * Service registration for MiniCluster factory
   */
  static {
    MiniCluster.registerService(HBaseService.class);
  }

  /**
   * Service configuration keys
   */
  public static final String MASTER_PORT_KEY = "hbase-master-port";
  public static final String REGIONSERVER_PORT_KEY = "hbase-regionserver-port";

  /**
   * The name of the HBase meta table, which we need to successfully scan before
   * considering the cluster launched.
   */
  private static final String HBASE_META_TABLE = "hbase:meta";

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private int zookeeperClientPort = 2828;
  private String bindIP = "127.0.0.1";
  private int masterPort = 60000;
  private int regionserverPort = 60020;

  /**
   * Embedded HBase cluster
   */
  private MiniHBaseCluster hbaseCluster;

  public HBaseService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }
    if (serviceConfig.contains(MiniCluster.ZK_PORT_KEY)) {
      zookeeperClientPort = Integer.parseInt(serviceConfig
          .get(MiniCluster.ZK_PORT_KEY));
    }
    if (serviceConfig.contains(MASTER_PORT_KEY)) {
      masterPort = Integer.parseInt(serviceConfig.get(MASTER_PORT_KEY));
    }
    if (serviceConfig.contains(REGIONSERVER_PORT_KEY)) {
      masterPort = Integer.parseInt(serviceConfig.get(REGIONSERVER_PORT_KEY));
    }
    hadoopConf = serviceConfig.getHadoopConf();
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException, InterruptedException {
    Preconditions.checkState(hadoopConf != null,
        "Hadoop Configuration must be set before starting mini HBase cluster");
    Preconditions.checkState(zookeeperClientPort != 0,
        "The zookeeper client port must be configured to a non zero value");

    // We first start an empty HBase cluster before fully configuring it
    hbaseCluster = new MiniHBaseCluster(hadoopConf, 0, 0, null, null);
    // Configure the cluster, and start a master and regionserver.
    hadoopConf = configureHBaseCluster(hbaseCluster.getConf(),
        zookeeperClientPort, FileSystem.get(hadoopConf), bindIP, masterPort,
        regionserverPort);
    hbaseCluster.startMaster();
    hbaseCluster.startRegionServer();
    waitForHBaseToComeOnline(hbaseCluster);
    logger.info("HBase Minicluster Service Started.");
  }

  @Override
  public void stop() throws IOException {
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
      this.hbaseCluster.killAll();
      this.hbaseCluster.waitUntilShutDown();
      logger.info("HBase Minicluster Service Shut Down.");
      this.hbaseCluster = null;
    }
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    List<Class<? extends Service>> services = new ArrayList<Class<? extends Service>>();
    services.add(HdfsService.class);
    services.add(ZookeeperService.class);
    return services;
  }

  /**
   * Configure the HBase cluster before launching it
   * 
   * @param config
   *          already created Hadoop configuration we'll further configure for
   *          HDFS
   * @param zkClientPort
   *          The client port zookeeper is listening on
   * @param hdfsFs
   *          The HDFS FileSystem this HBase cluster will run on top of
   * @param bindIP
   *          The IP Address to force bind all sockets on. If null, will use
   *          defaults
   * @param masterPort
   *          The port the master listens on
   * @param regionserverPort
   *          The port the regionserver listens on
   * @return The updated Configuration object.
   * @throws IOException
   */
  private static Configuration configureHBaseCluster(Configuration config,
      int zkClientPort, FileSystem hdfsFs, String bindIP, int masterPort,
      int regionserverPort) throws IOException {
    // Configure the zookeeper port
    config
        .set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkClientPort));
    // Initialize HDFS path configurations required by HBase
    Path hbaseDir = new Path(hdfsFs.makeQualified(hdfsFs.getHomeDirectory()),
        "hbase");
    FSUtils.setRootDir(config, hbaseDir);
    hdfsFs.mkdirs(hbaseDir);
    config.set("fs.defaultFS", hdfsFs.getUri().toString());
    config.set("fs.default.name", hdfsFs.getUri().toString());
    FSUtils.setVersion(hdfsFs, hbaseDir);

    // Configure the bind addresses and ports. If running in Openshift, we only
    // have permission to bind to the private IP address, accessible through an
    // environment variable.
    logger.info("HBase force binding to ip: " + bindIP);
    config.set("hbase.master.ipc.address", bindIP);
    config.set(HConstants.MASTER_PORT, Integer.toString(masterPort));
    config.set("hbase.regionserver.ipc.address", bindIP);
    config
        .set(HConstants.REGIONSERVER_PORT, Integer.toString(regionserverPort));
    config.set(HConstants.ZOOKEEPER_QUORUM, bindIP);

    // By default, the HBase master and regionservers will report to zookeeper
    // that its hostname is what it determines by reverse DNS lookup, and not
    // what we use as the bind address. This means when we set the bind
    // address, daemons won't actually be able to connect to eachother if they
    // are different. Here, we do something that's illegal in 48 states - use
    // reflection to override a private static final field in the DNS class
    // that is a cachedHostname. This way, we are forcing the hostname that
    // reverse dns finds. This may not be compatible with newer versions of
    // Hadoop.
    try {
      Field cachedHostname = DNS.class.getDeclaredField("cachedHostname");
      cachedHostname.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(cachedHostname, cachedHostname.getModifiers()
          & ~Modifier.FINAL);
      cachedHostname.set(null, bindIP);
    } catch (Exception e) {
      // Reflection can throw so many checked exceptions. Let's wrap in an
      // IOException.
      throw new IOException(e);
    }

    // By setting the info ports to -1 for, we won't launch the master or
    // regionserver info web interfaces
    config.set(HConstants.MASTER_INFO_PORT, "-1");
    config.set(HConstants.REGIONSERVER_INFO_PORT, "-1");
    return config;
  }

  /**
   * Wait for the hbase cluster to start up and come online, and then return.
   * 
   * @param hbaseCluster
   *          The hbase cluster to wait for.
   * @throws IOException
   */
  private static void waitForHBaseToComeOnline(MiniHBaseCluster hbaseCluster)
      throws IOException, InterruptedException {
    // Wait for the master to be initialized. This is required because even
    // before it's initialized, the regionserver can come online and the meta
    // table can be scannable. If the cluster is quickly shut down after all of
    // this before the master is initialized, it can cause the shutdown to hang
    // indefinitely as initialization tasks will block forever.
    //
    // Unfortunately, no method available to wait for master to come online like
    // regionservers, so we use a while loop with a sleep so we don't hammer the
    // isInitialized method.
    while (!hbaseCluster.getMaster().isInitialized()) {
      Thread.sleep(1000);
    }
    // Now wait for the regionserver to come online.
    hbaseCluster.getRegionServer(0).waitForServerOnline();
    // Don't leave here till we've done a successful scan of the hbase:meta
    // This validates that not only is the regionserver up, but that the
    // meta region is online so there are no race conditions where operations
    // requiring the meta region might run before it's available. Otherwise,
    // operations are susceptible to region not online errors.
    HTable t = new HTable(hbaseCluster.getConf(), HBASE_META_TABLE);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
  }
}
