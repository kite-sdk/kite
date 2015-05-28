/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Manages a default Hadoop {@link Configuration}.
 * <p>
 * Implementations that are built on Hadoop need to be passed options via a
 * {@code Configuration} that is based on the environment, but could be changed
 * by command-line options. This class allows both patterns. The {@link #get()}
 * method will return a {@code Configuration} based on the environment by
 * default, which can be updated and changed using {@link #set(Configuration)}.
 * <p>
 * Note that the {@code Configuration} managed by this class is global and must
 * only be used for unchanging configuration options, like the URI for HDFS.
 */
public class DefaultConfiguration {

  // initialize the default configuration from the environment
  private static boolean initDone = false;
  private static Configuration conf;

  static{
    //read system property for Oozie Action Configuration XML
    String oozieActionXml = System.getProperty("oozie.action.conf.xml");
    if(oozieActionXml != null && oozieActionXml.length() > 0) {
      Configuration.addDefaultResource(new Path("file:///", oozieActionXml).toString());
    }

    conf = new Configuration();
  }



  /**
   * Get a copy of the default Hadoop {@link Configuration}.
   * <p>
   * The {@code Configuration} returned by this method can be changed without
   * changing the global {@code Configuration}.
   *
   * @return A {@code Configuration} based on the environment or set by
   *          {@link #set(Configuration)}
   */
  public static synchronized Configuration get() {
    return new Configuration(conf);
  }

  /**
   * Set the default Hadoop {@link Configuration}.
   */
  public static synchronized void set(Configuration conf) {
    DefaultConfiguration.conf = conf;
    DefaultConfiguration.initDone = true;
  }

  /**
   * Initialize the default Hadoop {@link Configuration} if it has not been set.
   */
  public static synchronized void init(Configuration conf) {
    if (!initDone) {
      DefaultConfiguration.conf = conf;
      DefaultConfiguration.initDone = true;
    }
  }
}
