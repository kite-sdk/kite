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
import com.google.common.collect.Lists;

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.minicluster.Service.ServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A in process MiniCluster implementation for Kite that is configurable with
 * Services that make up the MiniCluster. Examples of Services are HDFS, HBase,
 * Zookeeper, etc...
 * 
 * This MiniCluster should be built with its internal Builder class, which will
 * validate proper configuration of the MiniCluster. For example, a Service can
 * have dependencies, so the builder will validate that proper Service
 * dependencies have been added to the MiniCluster.
 */
public class MiniCluster {

  private static final Logger logger = LoggerFactory
      .getLogger(MiniCluster.class);

  public static final String BIND_IP_KEY = "bind-ip";
  public static final String CLEAN_KEY = "clean";
  public static final String WORK_DIR_KEY = "directory";
  public static final String NAMENODE_RPC_PORT = "hdfs-namenode-rpc-port";
  public static final String ZK_PORT_KEY = "zk-port";
  public static final String HIVE_METASTORE_PORT = "hive-metastore-port";
  public static final String HIVE_SERVER_PORT = "hive-server-port";
  public static final String FLUME_CONFIGURATION = "flume-configuration";
  public static final String FLUME_AGENT_NAME = "flume-agent-name";

  static final String RESOURCE_URI_SCHEME = "resource";

  private static final Map<String, Service> registeredServices = new ConcurrentHashMap<String, Service>();

  private final List<Service> services;
  private final ServiceConfig serviceConfig;

  public static void registerService(Class<? extends Service> klass) {
    Service service;
    try {
      service = klass.getConstructor().newInstance();
    } catch (Exception e) {
      logger.error("Could not get default class constructor for: "
          + klass.getName());
      throw new RuntimeException(e);
    }
    registeredServices.put(klass.getName(), service);
  }

  /**
   * Private constructor. The MiniCluster should be built with the Builder
   * class.
   * 
   * @param services
   *          cluster services The services in run order
   * @param serviceConfig
   *          The Hadoop Configuration to start running the mini cluster
   *          services with.
   */
  private MiniCluster(List<Service> services, ServiceConfig serviceConfig) {
    this.services = services;
    this.serviceConfig = serviceConfig;
  }

  /**
   * A MiniCluster Builder.
   */
  public static class Builder {

    private Configuration hadoopConf;
    private List<Service> services = Lists.newArrayList();
    private ServiceConfig serviceConfig = new ServiceConfig();

    public Builder hadoopConf(Configuration hadoopConf) {
      serviceConfig.setHadoopConf(hadoopConf);
      return this;
    }

    public Builder workDir(String workDir) {
      serviceConfig.set(WORK_DIR_KEY, workDir);
      return this;
    }

    public Builder clean(boolean clean) {
      serviceConfig.set(CLEAN_KEY, Boolean.toString(clean));
      return this;
    }

    public Builder bindIP(String bindIP) {
      serviceConfig.set(BIND_IP_KEY, bindIP);
      return this;
    }
    
    public Builder namenodeRpcPort(int namenodeRpcPort) {
      serviceConfig.set(NAMENODE_RPC_PORT, Integer.toString(namenodeRpcPort));
      return this;
    }
    
    public Builder zkPort(int zkPort) {
      serviceConfig.set(ZK_PORT_KEY, Integer.toString(zkPort));
      return this;
    }

    public Builder hiveMetastorePort(int port) {
      serviceConfig.set(HIVE_METASTORE_PORT, Integer.toString(port));
      return this;
    }

    public Builder hiveServerPort(int port) {
      serviceConfig.set(HIVE_SERVER_PORT, Integer.toString(port));
      return this;
    }

    public Builder flumeConfiguration(String resource) {
      serviceConfig.set(FLUME_CONFIGURATION, toUrl(resource).toExternalForm());
      return this;
    }

    public Builder flumeAgentName(String name) {
      serviceConfig.set(FLUME_AGENT_NAME, name);
      return this;
    }

    private URL toUrl(String resource) {
      URI resourceUri = URI.create(resource);
      if (RESOURCE_URI_SCHEME.equals(resourceUri.getScheme())) {
        // following throws IllegalArgumentException if resource isn't found
        return Resources.getResource(resourceUri.getRawSchemeSpecificPart());
      } else { // treat as file path
        File file = new File(resource);
        if (!file.exists()) {
          throw new IllegalArgumentException(String.format("File %s not found.", file));
        }
        try {
          return file.toURI().toURL();
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }

    /**
     * Service configs are dynamic config name-value pairs that can be
     * interpreted by the services. This allows new services to be added that
     * have their own encapsulated configuration parameters that don't need to
     * be exposed statically from this Builder.
     * 
     * @param name
     *          The name of the configuration parameter
     * @param value
     *          The value of the config setting
     * @return this Builder for method chaining.
     */
    public Builder setServiceConfig(String name, String value) {
      serviceConfig.set(name, value);
      return this;
    }

    public Builder addService(Class<? extends Service> klass) {
      Preconditions.checkState(!serviceImplExists(klass),
          "A service implementation already exists for: " + klass.getName());
      try {
        Class.forName(klass.getName());
      } catch (ClassNotFoundException e) {
        // ignore
      }
      Service service = registeredServices.get(klass.getName());
      Preconditions.checkState(service != null,
          "Unknown service (maybe not registered): " + klass.getName());
      services.add(service);
      return this;
    }

    public MiniCluster build() {
      Preconditions.checkState(serviceConfig.get(WORK_DIR_KEY) != null,
          "Must provide a path on the local filesystem to store cluster data");

      if (hadoopConf == null) {
        hadoopConf = new Configuration();
      }
      // Make the services list in run order based on each service's deps
      services = getServicesInRunOrder(services);

      // Return the configured mini cluster
      return new MiniCluster(services, serviceConfig);
    }

    /**
     * Given a list of services, sort them in the proper order based on their
     * run dependencies reported by the service's dependencies() method.
     * 
     * Will throw an IllegalStateException if required dependencies for a
     * service isn't in the list.
     * 
     * @return The ordered Service list.
     */
    private List<Service> getServicesInRunOrder(List<Service> services) {
      List<Service> orderedServices = new ArrayList<Service>();
      List<Service> serviceQueue = new ArrayList<Service>(services);
      while (orderedServices.size() < services.size()) {
        List<Integer> serviceQueueIndexesToRemove = new ArrayList<Integer>();
        for (int i = 0; i < serviceQueue.size(); i++) {
          Service service = serviceQueue.get(i);
          boolean allDependenciesIn = true;
          if (service.dependencies() != null) {
            for (Class<? extends Service> serviceClass : service.dependencies()) {
              if (!serviceImplExists(orderedServices, serviceClass)) {
                allDependenciesIn = false;
                break;
              }
            }
          }
          if (allDependenciesIn) {
            serviceQueueIndexesToRemove.add(i);
            orderedServices.add(service);
          }
        }
        if (serviceQueueIndexesToRemove.size() == 0) {
          // Indicates nothing to be moved. This means we have dependencies that
          // have not been added. Throw an exception.
          throw new IllegalStateException(
              "Required service dependencies haven't been added as services.");
        } else {
          int numRemoved = 0;
          for (int idx : serviceQueueIndexesToRemove) {
            serviceQueue.remove(idx - numRemoved++);
          }
        }
      }
      return orderedServices;
    }

    @SuppressWarnings("unchecked")
    private <T extends Service> T getServiceImpl(List<Service> services,
        Class<T> klass) {
      for (Service service : services) {
        if (service.getClass() == klass) {
          return (T) service;
        }
      }
      return null;
    }

    private boolean serviceImplExists(Class<? extends Service> klass) {
      return serviceImplExists(services, klass);
    }

    private boolean serviceImplExists(List<Service> services,
        Class<? extends Service> klass) {
      return getServiceImpl(services, klass) != null;
    }

  }

  /**
   * Starts the services in order, passing the previous service's modified
   * Configuration object to the next.
   * 
   * @throws IOException
   */
  public void start() throws IOException, InterruptedException {
    for (Service service : services) {
      service.configure(serviceConfig);
      logger.info("Running Minicluster Service: "
          + service.getClass().getName());
      service.start();
      serviceConfig.setHadoopConf(service.getHadoopConf());
      // set the default configuration so that the minicluster is used
      DefaultConfiguration.set(serviceConfig.getHadoopConf());
    }
    logger.info("All Minicluster Services running.");
  }

  /**
   * Stops the services in reverse of their run order.
   * 
   * @throws IOException
   */
  public void stop() throws IOException, InterruptedException {
    for (int i = services.size() - 1; i >= 0; i--) {
      Service service = services.get(i);
      logger.info("Stopping Minicluster Service: "
          + service.getClass().getName());
      service.stop();
    }
    logger.info("All Minicluster Services stopped.");
  }
}
