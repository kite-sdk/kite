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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A in process MiniCluster implementation for Kite that is configurable with
 * Services that make up the MiniCluster. Examples of Services are HDFS, HBase,
 * Zookeeper, etc...
 * 
 * This MiniCluster should be built with it's internal Builder class, which will
 * validate proper configuration of the MiniCluster. For example, a Service can
 * have dependencies, so the builder will validate that proper Service
 * dependencies have been added to the MiniCluster.
 */
public class MiniCluster {

  private static final Logger logger = LoggerFactory
      .getLogger(MiniCluster.class);

  private final List<Service> services;
  private final Configuration conf;

  /**
   * Private constructor. The MiniCluster should be built with the Builder
   * class.
   * 
   * @param mini
   *          cluster services The services in run order
   * @param conf
   *          The Hadoop Configuration to start running the mini cluster
   *          services with.
   */
  private MiniCluster(List<Service> services, Configuration conf) {
    this.services = services;
    this.conf = conf;
  }

  /**
   * A MiniCluster Builder.
   */
  public static class Builder {

    private boolean clean = false;
    private String localBaseFsLocation;
    private String forceBindIP;
    private Configuration conf;
    private List<Service> services = Lists.newArrayList();

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder localBaseFsLocation(String localBaseFsLocation) {
      this.localBaseFsLocation = localBaseFsLocation;
      return this;
    }

    public Builder forceBindIP(String forceBindIP) {
      this.forceBindIP = forceBindIP;
      return this;
    }

    public Builder clean(boolean clean) {
      this.clean = clean;
      return this;
    }

    public Builder addHdfsService() {
      Preconditions.checkState(!serviceImplExists(HdfsService.class),
          "An HdfsService implementation has already been added. "
              + "There can only be one.");
      services.add(new HdfsService());
      return this;
    }

    public Builder addHBaseService() {
      Preconditions.checkState(!serviceImplExists(HBaseService.class),
          "An HBaseService implementation has already been added. "
              + "There can only be one.");
      services.add(new HBaseService());
      return this;
    }

    public Builder addZookeeperService(int clientPort) {
      Preconditions.checkState(!serviceImplExists(ZookeeperService.class),
          "An ZookeeperService implementation has already been added. "
              + "There can only be one.");
      services.add(new ZookeeperService(clientPort));
      return this;
    }

    public MiniCluster build() {
      Preconditions.checkState(localBaseFsLocation != null,
          "Must provide a path on the local filesystem to store cluster data");

      if (conf == null) {
        conf = new Configuration();
      }
      // Make the services list in run order based on each service's deps
      services = getServicesInRunOrder(services);
      // Configure each service
      for (Service service : services) {
        logger.info("Configuring " + service.getClass().getName());
        configureService(service);
      }
      // Return the configured mini cluster
      return new MiniCluster(services, conf);
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

    private <T extends Service> T getServiceImpl(Class<T> klass) {
      return getServiceImpl(services, klass);
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

    private void configureService(Service service) {
      if (service.getClass() == HdfsService.class) {
        configureHdfs((HdfsService) service);
      } else if (service.getClass() == ZookeeperService.class) {
        configureZookeeper((ZookeeperService) service);
      } else if (service.getClass() == HBaseService.class) {
        configureHBase((HBaseService) service);
      }
    }

    private void configureHdfs(HdfsService service) {
      service.setLocalBaseFsLocation(localBaseFsLocation);
      service.setForceBindIP(forceBindIP);
      service.setClean(clean);
    }

    private void configureZookeeper(ZookeeperService service) {
      service.setLocalBaseFsLocation(localBaseFsLocation);
      service.setForceBindIP(forceBindIP);
      service.setClean(clean);
    }

    private void configureHBase(HBaseService service) {
      service.setForceBindIP(forceBindIP);
      // Configure the zookeeper client port of HBase with the zookeeper
      // service's configured client port
      service.setZookeeperClientPort(getServiceImpl(ZookeeperService.class)
          .getClientPort());
    }
  }

  /**
   * Starts the services in order, passing the previous service's modified
   * Configuration object to the next.
   * 
   * @throws IOException
   */
  public void start() throws IOException {
    Configuration runningConf = conf;
    for (Service service : services) {
      service.setConf(runningConf);
      logger.info("Running Minicluster Service: "
          + service.getClass().getName());
      service.start();
      runningConf = service.getConf();
    }
    logger.info("All Minicluster Services running.");
  }

  /**
   * Stops the services in reverse of their run order.
   * 
   * @throws IOException
   */
  public void stop() throws IOException {
    for (int i = services.size() - 1; i >= 0; i--) {
      Service service = services.get(i);
      logger.info("Stopping Minicluster Service: "
          + service.getClass().getName());
      service.stop();
    }
    logger.info("All Minicluster Services stopped.");
  }
}
