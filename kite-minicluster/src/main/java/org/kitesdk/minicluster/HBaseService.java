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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
  
  private static final String HBASE_META_TABLE = "hbase:meta";
  private static final String MANAGED_SCHEMAS_TABLE = "managed_schemas";

  private Configuration conf;
  private Integer zookeeperClientPort = null;
  private String forceBindIP;
  private MiniHBaseCluster hbaseCluster;

  @Override
  public void start() throws IOException {
    Preconditions.checkState(conf != null,
        "Configuration must be set before starting mini HBase cluster");
    Preconditions.checkState(zookeeperClientPort != null,
        "The zookeeper client port must be configured");

    // We first start an empty HBase cluster before fully configuring it
    try {
      hbaseCluster = new MiniHBaseCluster(conf, 0, 0, null, null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    // Configure the cluster, and start a master and regionserver.
    conf = configureHBaseCluster(hbaseCluster.getConf(), zookeeperClientPort,
        FileSystem.get(conf), forceBindIP);
    hbaseCluster.startMaster();
    hbaseCluster.startRegionServer();
    waitForHBaseToComeOnline(hbaseCluster);
    // Create system tables required by Kite
    createManagedSchemasTable(conf);
    logger.info("HBase Minicluster Service Started.");
  }

  @Override
  public void stop() throws IOException {
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
      this.hbaseCluster.waitUntilShutDown();
      logger.info("HBase Minicluster Service Shut Down.");
      this.hbaseCluster = null;
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    List<Class<? extends Service>> services = new ArrayList<Class<? extends Service>>();
    services.add(HdfsService.class);
    services.add(ZookeeperService.class);
    return services;
  }

  /**
   * Set the port zookeeper is listening for client connections on.
   * 
   * @param zookeeperClientPort
   */
  void setZookeeperClientPort(int zookeeperClientPort) {
    this.zookeeperClientPort = zookeeperClientPort;
  }

  /**
   * Set an IP address for all sockets that listen to bind to. Useful for
   * environments where you are restricted by which IP addresses you are allowed
   * to bind to.
   * 
   * @param forceBindIP
   */
  void setForceBindIP(String forceBindIP) {
    this.forceBindIP = forceBindIP;
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
   * @param forceBindIP
   *          The IP Address to force bind all sockets on. If null, will use
   *          defaults
   * @return The updated Configuration object.
   * @throws IOException
   */
  private static Configuration configureHBaseCluster(Configuration config,
      int zkClientPort, FileSystem hdfsFs, String forceBindIP)
      throws IOException {
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
    if (forceBindIP != null) {
      logger.info("HBase force binding to ip: " + forceBindIP);
      config.set("hbase.master.ipc.address", forceBindIP);
      config.set("hbase.regionserver.ipc.address", forceBindIP);
      config.set(HConstants.ZOOKEEPER_QUORUM, forceBindIP);

      // By default, the HBase master and regionservers will report to zookeeper
      // that it's hostname is what it determines by reverse DNS lookup, and not
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
        cachedHostname.set(null, forceBindIP);
      } catch (Exception e) {
        // Reflection can throw so many checked exceptions. Let's wrap in an
        // IOException.
        throw new IOException(e);
      }
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
      throws IOException {
    // wait for regionserver to come online, and then break out of loop.
    while (true) {
      if (hbaseCluster.getRegionServer(0).isOnline()) {
        break;
      }
    }
    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(hbaseCluster.getConf(), HBASE_META_TABLE);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
  }

  /**
   * Create the required HBase tables for the Kite HBase module. If those are
   * already initialized, this method will do nothing.
   * 
   * @param config
   *          The HBase configuration
   */
  private static void createManagedSchemasTable(Configuration config)
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(config);
    try {
      if (!admin.tableExists(MANAGED_SCHEMAS_TABLE)) {
        logger.info("Created Table: " + MANAGED_SCHEMAS_TABLE);
        @SuppressWarnings("deprecation")
        HTableDescriptor desc = new HTableDescriptor(MANAGED_SCHEMAS_TABLE);
        desc.addFamily(new HColumnDescriptor("meta"));
        desc.addFamily(new HColumnDescriptor("schema"));
        desc.addFamily(new HColumnDescriptor("_s"));
        admin.createTable(desc);
      }
    } finally {
      admin.close();
    }
  }
}
