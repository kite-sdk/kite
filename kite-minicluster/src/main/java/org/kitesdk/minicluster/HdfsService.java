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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HDFS minicluster service implementation.
 */
public class HdfsService implements Service {

  private static final Logger logger = LoggerFactory
      .getLogger(HdfsService.class);

  /**
   * Service registration for MiniCluster factory
   */
  static {
    MiniCluster.registerService(HdfsService.class);
  }

  /**
   * Service configuration keys
   */
  public static final String NAMENODE_HTTP_PORT = "hdfs-namenode-http-port";
  public static final String DATANODE_PORT = "hdfs-datanode-port";
  public static final String DATANODE_IPC_PORT = "hdfs-datanode-ipc-port";
  public static final String DATANODE_HTTP_PORT = "hdfs-datanode-http-port";

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String workDir;
  private String bindIP = "127.0.0.1";
  private int namenodeRpcPort = 8020;
  private int namenodeHttpPort = 50070;
  private int datanodePort = 50010;
  private int datanodeIpcPort = 50020;
  private int datanodeHttpPort = 50075;
  private boolean clean = false;

  /**
   * Embedded HDFS cluster
   */
  private MiniDFSCluster miniDfsCluster;

  public HdfsService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    this.workDir = serviceConfig.get(MiniCluster.WORK_DIR_KEY);
    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }
    if (serviceConfig.contains(MiniCluster.CLEAN_KEY)) {
      clean = Boolean.parseBoolean(serviceConfig.get(MiniCluster.CLEAN_KEY));
    }
    if (serviceConfig.contains(MiniCluster.NAMENODE_RPC_PORT)) {
      namenodeRpcPort = Integer.parseInt(serviceConfig
          .get(MiniCluster.NAMENODE_RPC_PORT));
    }
    if (serviceConfig.contains(NAMENODE_HTTP_PORT)) {
      namenodeHttpPort = Integer
          .parseInt(serviceConfig.get(NAMENODE_HTTP_PORT));
    }
    if (serviceConfig.contains(DATANODE_PORT)) {
      datanodePort = Integer.parseInt(serviceConfig.get(DATANODE_PORT));
    }
    if (serviceConfig.contains(DATANODE_IPC_PORT)) {
      datanodeIpcPort = Integer.parseInt(serviceConfig.get(DATANODE_IPC_PORT));
    }
    if (serviceConfig.contains(DATANODE_HTTP_PORT)) {
      datanodeHttpPort = Integer
          .parseInt(serviceConfig.get(DATANODE_HTTP_PORT));
    }
    hadoopConf = serviceConfig.getHadoopConf();
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException {
    Preconditions.checkState(workDir != null,
        "The work dir must be set before starting cluster.");

    if (hadoopConf == null) {
      hadoopConf = new Configuration();
    }

    // If clean, then remove the work dir so we can start fresh.
    String localDFSLocation = getDFSLocation(workDir);
    if (clean) {
      logger.info("Cleaning HDFS cluster data at: " + localDFSLocation
          + " and starting fresh.");
      File file = new File(localDFSLocation);
      FileUtils.deleteDirectory(file);
    }

    // Configure and start the HDFS cluster
    boolean format = shouldFormatDFSCluster(localDFSLocation, clean);
    hadoopConf = configureDFSCluster(hadoopConf, localDFSLocation, bindIP,
        namenodeRpcPort, namenodeHttpPort, datanodePort, datanodeIpcPort,
        datanodeHttpPort);
    miniDfsCluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(1)
        .format(format).checkDataNodeAddrConfig(true)
        .checkDataNodeHostConfig(true).build();
    logger.info("HDFS Minicluster service started.");
  }

  @Override
  public void stop() throws IOException {
    miniDfsCluster.shutdown();
    logger.info("HDFS Minicluster service shut down.");
    miniDfsCluster = null;
    hadoopConf = null;
  }

  /**
   * Get the location on the local FS where we store the HDFS data.
   * 
   * @param baseFsLocation
   *          The base location on the local filesystem we have write access to
   *          create dirs.
   * @return The location for HDFS data.
   */
  private static String getDFSLocation(String baseFsLocation) {
    return baseFsLocation + Path.SEPARATOR + "dfs";
  }

  /**
   * Returns true if we should format the DFS Cluster. We'll format if clean is
   * true, or if the dfsFsLocation does not exist.
   * 
   * @param localDFSLocation
   *          The location on the local FS to hold the HDFS metadata and block
   *          data
   * @param clean
   *          Specifies if we want to start a clean cluster
   * @return Returns true if we should format a DFSCluster, otherwise false
   */
  private static boolean shouldFormatDFSCluster(String localDFSLocation,
      boolean clean) {
    boolean format = true;
    File f = new File(localDFSLocation);
    if (f.exists() && f.isDirectory() && !clean) {
      format = false;
    }
    return format;
  }

  /**
   * Configure the DFS Cluster before launching it.
   * 
   * @param config
   *          The already created Hadoop configuration we'll further configure
   *          for HDFS
   * @param localDFSLocation
   *          The location on the local filesystem where cluster data is stored
   * @param bindIP
   *          An IP address we want to force the datanode and namenode to bind
   *          to.
   * @param namenodeRpcPort
   * @param namenodeHttpPort
   * @param datanodePort
   * @param datanodeIpcPort
   * @param datanodeHttpPort
   * @return The updated Configuration object.
   */
  private static Configuration configureDFSCluster(Configuration config,
      String localDFSLocation, String bindIP, int namenodeRpcPort,
      int namenodeHttpPort, int datanodePort, int datanodeIpcPort,
      int datanodeHttpPort) {

    logger.info("HDFS force binding to ip: " + bindIP);
    config = new KiteCompatibleConfiguration(config, bindIP, namenodeRpcPort,
        namenodeHttpPort);
    config.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + bindIP + ":"
        + namenodeRpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, bindIP + ":"
        + datanodePort);
    config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, bindIP + ":"
        + datanodeIpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, bindIP + ":"
        + datanodeHttpPort);
    // When a datanode registers with the namenode, the Namenode do a hostname
    // check of the datanode which will fail on OpenShift due to reverse DNS
    // issues with the internal IP addresses. This config disables that check,
    // and will allow a datanode to connect regardless.
    config.setBoolean("dfs.namenode.datanode.registration.ip-hostname-check",
        false);
    config.set("hdfs.minidfs.basedir", localDFSLocation);
    // allow current user to impersonate others
    String user = System.getProperty("user.name");
    config.set("hadoop.proxyuser." + user + ".groups", "*");
    config.set("hadoop.proxyuser." + user + ".hosts", "*");
    return config;
  }

  /**
   * A Hadoop Configuration class that won't override the Namenode RPC and
   * Namenode HTTP bind addresses. The mini DFS cluster sets this bind address
   * to 127.0.0.1, and this can't be overridden. In environments where you can't
   * bind to 127.0.0.1 (like OpenShift), this will not work, so make sure these
   * settings can't be overridden by the mini DFS cluster.
   */
  private static class KiteCompatibleConfiguration extends Configuration {

    public KiteCompatibleConfiguration(Configuration config,
        String bindAddress, int namenodeRpcPort, int namenodeHttpPort) {
      super(config);
      super.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, bindAddress + ":"
          + namenodeRpcPort);
      super.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, bindAddress + ":"
          + namenodeHttpPort);
    }

    @Override
    public void set(String key, String value) {
      if (!key.equals(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY)
          && !key.equals(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY)) {
        super.set(key, value);
      }
    }
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    // no dependencies
    return null;
  }
}
