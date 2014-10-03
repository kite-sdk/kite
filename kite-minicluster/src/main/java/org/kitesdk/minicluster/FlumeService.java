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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flume.node.Application;
import org.apache.flume.node.PropertiesFileConfigurationProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kitesdk.minicluster.MiniCluster.FLUME_AGENT_NAME;
import static org.kitesdk.minicluster.MiniCluster.FLUME_CONFIGURATION;

public class FlumeService implements Service {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeService.class);

  /**
   * Service registration for MiniCluster factory
   */
  static {
    MiniCluster.registerService(FlumeService.class);
  }

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String workDir;
  private String bindIP = "127.0.0.1";
  private String flumeConfiguration;
  private String agentName;
  private Application flumeApplication;

  public FlumeService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    Preconditions.checkState(serviceConfig.contains(FLUME_CONFIGURATION),
        "Missing configuration for Flume minicluster: " + FLUME_CONFIGURATION);
    Preconditions.checkState(serviceConfig.contains(FLUME_AGENT_NAME),
        "Missing configuration for Flume minicluster: " + FLUME_AGENT_NAME);
    this.workDir = serviceConfig.get(MiniCluster.WORK_DIR_KEY);
    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }
    this.flumeConfiguration = serviceConfig.get(FLUME_CONFIGURATION);
    this.agentName = serviceConfig.get(FLUME_AGENT_NAME);
    hadoopConf = serviceConfig.getHadoopConf(); // not used
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException {
    File newFlumeConfiguration = configureBindIp(flumeConfiguration);
    PropertiesFileConfigurationProvider configurationProvider =
        new PropertiesFileConfigurationProvider(agentName, newFlumeConfiguration);
    flumeApplication = new Application();
    flumeApplication.handleConfigurationEvent(configurationProvider.getConfiguration());
    flumeApplication.start();
    LOG.info("Flume Minicluster service started.");
  }

  private File configureBindIp(String flumeConfiguration) throws IOException {
    Properties properties = readFromUrl(flumeConfiguration);
    fixBindAddresses(properties, bindIP);
    File flumeWorkDir = new File(workDir, "flume");
    flumeWorkDir.mkdirs();
    File newFlumeConfiguration = new File(flumeWorkDir, "flume.properties");
    writeToFile(properties, newFlumeConfiguration);
    return newFlumeConfiguration;
  }

  private Properties readFromUrl(String flumeConfiguration) throws IOException {
    BufferedReader reader = null;
    try {
      URL url = new URL(flumeConfiguration);
      reader = new BufferedReader(new InputStreamReader(url.openStream()));
      Properties properties = new Properties();
      properties.load(reader);
      return properties;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
          LOG.warn("Unable to close reader for: " + flumeConfiguration, ex);
        }
      }
    }
  }

  private void writeToFile(Properties properties, File file) throws IOException {
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(file));
      properties.store(writer, null);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ex) {
          LOG.warn("Unable to close file writer for file: " + file, ex);
        }
      }
    }
  }

  void fixBindAddresses(Properties properties, String bindIP) {
    for (String name : properties.stringPropertyNames()) {
      if (name.endsWith(".bind")) {
        properties.setProperty(name, bindIP);
      }
    }
  }

  @Override
  public void stop() throws IOException {
    flumeApplication.stop();
    LOG.info("Flume Minicluster service shut down.");
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    List<Class<? extends Service>> services = new ArrayList<Class<? extends Service>>();
    services.add(HdfsService.class);
    services.add(HiveService.class); // TODO: make this optional
    return services;
  }
}
