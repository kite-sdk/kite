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

  private String localBaseFsLocation;
  private String forceBindIP;
  private boolean clean = false;

  private Configuration conf;
  private MiniDFSCluster miniDfsCluster;

  @Override
  public void start() throws IOException {
    Preconditions.checkState(localBaseFsLocation != null,
        "The localBaseFsLocation must be set before starting cluster.");

    if (conf == null) {
      conf = new Configuration();
    }

    // If clean, then remove the localFsLocation so we can start fresh.
    String localDFSLocation = getDFSLocation(localBaseFsLocation);
    if (clean) {
      logger.info("Cleaning HDFS cluster data at: " + localDFSLocation
          + " and starting fresh.");
      File file = new File(localDFSLocation);
      FileUtils.deleteDirectory(file);
    }

    // Configure and start the HDFS cluster
    boolean format = shouldFormatDFSCluster(localDFSLocation, clean);
    conf = configureDFSCluster(conf, localDFSLocation, forceBindIP);
    miniDfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(format).checkDataNodeAddrConfig(true)
        .checkDataNodeHostConfig(true).build();
    logger.info("HDFS Minicluster service started.");
  }

  @Override
  public void stop() throws IOException {
    miniDfsCluster.shutdown();
    logger.info("HDFS Minicluster service shut down.");
    miniDfsCluster = null;
    conf = null;
  }

  @Override
  public Configuration getConf() {
    return conf;
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
   * @param forceBindIP
   *          An IP address we want to force the datanode and namenode to bind
   *          to.
   * @return The updated Configuration object.
   */
  private static Configuration configureDFSCluster(Configuration config,
      String localDFSLocation, String forceBindIP) {

    // If running in env where we only have permission to bind to certain IP
    // address, force very socket that listens to bind to that IP address.
    if (forceBindIP != null) {
      logger.info("HDFS force binding to ip: " + forceBindIP);
      config = new OpenshiftCompatibleConfiguration(config, forceBindIP);
      config.set("dfs.datanode.address", forceBindIP + ":50010");
      config.set("dfs.datanode.http.address", forceBindIP + ":50075");
      config.set("dfs.datanode.ipc.address", forceBindIP + ":50020");
      // When a datanode registers with the namenode, the Namenode do a hostname
      // check of the datanode which will fail on OpenShift due to reverse DNS
      // issues with the internal IP addresses. This config disables that check,
      // and will allow a datanode to connect regardless.
      config.setBoolean("dfs.namenode.datanode.registration.ip-hostname-check",
          false);
    }
    config.set("hdfs.minidfs.basedir", localDFSLocation);
    return config;
  }

  /**
   * A Hadoop Configuration class that won't override the Namenode RPC and
   * Namenode HTTP bind addresses. The mini DFS cluster sets this bind address
   * to 127.0.0.1, and this can't be overridden. In environments where you 
   * can't bind to 127.0.0.1 (like OpenShift), this will not work, so make sure
   * these settings can't be overridden by the mini DFS cluster.
   */
  private static class OpenshiftCompatibleConfiguration extends Configuration {

    public OpenshiftCompatibleConfiguration(Configuration config,
        String bindAddress) {
      super(config);
      super.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, bindAddress
          + ":8020");
      super.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, bindAddress
          + ":50070");
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

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  void setLocalBaseFsLocation(String localBaseFsLocation) {
    this.localBaseFsLocation = localBaseFsLocation;
  }

  void setForceBindIP(String forceBindIP) {
    this.forceBindIP = forceBindIP;
  }

  void setClean(boolean clean) {
    this.clean = clean;
  }
}
