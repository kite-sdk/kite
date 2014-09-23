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
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.node.Application;
import org.apache.flume.node.PropertiesFileConfigurationProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kitesdk.minicluster.MiniCluster.FLUME_AGENT_NAME;
import static org.kitesdk.minicluster.MiniCluster.FLUME_CONFIGURATION;

class FlumeService implements Service {

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
  private PropertiesFileConfigurationProvider configurationProvider;
  private Application flumeApplication;

  public FlumeService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    Preconditions.checkState(serviceConfig.contains(FLUME_CONFIGURATION),
        "Missing configuration for Flume minicluster: " + FLUME_CONFIGURATION);
    Preconditions.checkState(serviceConfig.contains(FLUME_AGENT_NAME),
        "Missing configuration for Flume minicluster: " + FLUME_AGENT_NAME);
    File flumeConfiguration = new File(serviceConfig.get(FLUME_CONFIGURATION));
    String agentName = serviceConfig.get(FLUME_AGENT_NAME);
    configurationProvider = new PropertiesFileConfigurationProvider(agentName,
        flumeConfiguration);
    flumeApplication = new Application();
    hadoopConf = serviceConfig.getHadoopConf(); // not used
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException {
    flumeApplication.handleConfigurationEvent(configurationProvider.getConfiguration());
    flumeApplication.start();
    LOG.info("Flume Minicluster service started.");
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
