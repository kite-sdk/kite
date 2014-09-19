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
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface for MiniCluster Service implementations
 */
public interface Service {

  /**
   * Start the service.
   * 
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Stop the service.
   * 
   * @throws IOException
   */
  void stop() throws IOException;

  /**
   * Get the Hadoop Configuration this service is configured with
   * 
   * @return The Hadoop Configuration
   */
  Configuration getConf();

  /**
   * Set the Hadoop Configuration for this service
   * 
   * @param conf
   *          The Hadoop Configuration.
   */
  void setConf(Configuration conf);

  /**
   * Get the other Service implementations this Service has as a dependency. A
   * mini cluster will validate that the dependencies are added, and that they
   * are started before this one.
   * 
   * @return The list of service dependencies.
   */
  List<Class<? extends Service>> dependencies();
}
