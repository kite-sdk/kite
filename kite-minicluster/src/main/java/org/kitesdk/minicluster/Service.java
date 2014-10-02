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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface for MiniCluster Service implementations. Service implementation are
 * generally constructed by the MiniCluster class.
 * 
 * For this to be possible, implementations are expected to have a default
 * constructor, and can automatically register themselves with the MiniCluster
 * factory by having the static initializer:
 * 
 * <pre>
 * 
 * static {
 *   MiniCluster.registerService(MyService.class);
 * }
 * 
 * <pre>
 * 
 */
public interface Service {

  /**
   * Configure this service with the ServiceConfig
   * 
   * @param serviceConfig
   *          A Service Config instance
   */
  void configure(ServiceConfig serviceConfig);

  /**
   * Get the Hadoop configuration object for this service.
   * 
   * @return The Hadoop configuration
   */
  Configuration getHadoopConf();

  /**
   * Start the service.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void start() throws IOException, InterruptedException;

  /**
   * Stop the service.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void stop() throws IOException, InterruptedException;

  /**
   * Get the other Service implementations this Service has as a dependency. A
   * mini cluster will validate that the dependencies are added, and that they
   * are started before this one.
   * 
   * @return The list of service dependencies.
   */
  List<Class<? extends Service>> dependencies();

  /**
   * A class that holds configuration settings for minicluster services.
   */
  public static class ServiceConfig {

    private final Map<String, String> config = new HashMap<String, String>();
    private Configuration hadoopConf;

    public void setHadoopConf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
    }

    public Configuration getHadoopConf() {
      return hadoopConf;
    }

    public void set(String name, String value) {
      config.put(name, value);
    }

    public String get(String name) {
      return config.get(name);
    }

    public boolean contains(String name) {
      return config.containsKey(name);
    }
  }
}
