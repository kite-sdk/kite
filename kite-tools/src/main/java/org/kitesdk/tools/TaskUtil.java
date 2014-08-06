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

package org.kitesdk.tools;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;

public class TaskUtil {

  /**
   * Returns a configuration builder for the given {@link Job}.
   * @param job a {@code Job} to configure
   */
  public static ConfigBuilder configure(Job job) {
    return new ConfigBuilder(job);
  }

  /**
   * Returns a configuration builder for the given {@link Configuration}.
   * @param conf a {@code Configuration} to configure
   */
  public static ConfigBuilder configure(Configuration conf) {
    return new ConfigBuilder(conf);
  }

  public static class ConfigBuilder {
    private final Configuration conf;
    // this is needed because local distributed cache fails on jar files
    private final boolean skipDistributedCache;

    private ConfigBuilder(Job job) {
      this(Hadoop.JobContext.getConfiguration.<Configuration>invoke(job));
    }

    private ConfigBuilder(Configuration conf) {
      this.conf = conf;
      this.skipDistributedCache = conf.getBoolean("kite.testing", false);
    }

    /**
     * Finds the jar that contains the required class and adds it to the
     * distributed cache configuration.
     *
     * @param requiredClass a class required for a MR job
     * @return this for method chaining
     */
    public ConfigBuilder addJarForClass(Class<?> requiredClass) {
      if (!skipDistributedCache) {
        File jar = findJarForClass(requiredClass);
        try {
          DistCache.addJarToDistributedCache(conf, jar);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Cannot add jar to distributed cache: " + jar, e);
        }
      }
      return this;
    }

    /**
     * Finds the jar that contains the required class and adds its containing
     * directory to the distributed cache configuration.
     *
     * @param requiredClass a class required for a MR job
     * @return this for method chaining
     */
    public ConfigBuilder addJarPathForClass(Class<?> requiredClass) {
      if (!skipDistributedCache) {
        String jarPath = findJarForClass(requiredClass).getParent();
        try {
          DistCache.addJarDirToDistributedCache(conf, jarPath);
        } catch (IOException e) {
          throw new DatasetIOException(
              "Cannot add jar path to distributed cache: " + jarPath, e);
        }
      }
      return this;
    }
  }

  private static File findJarForClass(Class<?> requiredClass) {
    ProtectionDomain domain = AccessController.doPrivileged(
        new GetProtectionDomain(requiredClass));
    CodeSource codeSource = domain.getCodeSource();
    if (codeSource != null) {
      try {
        return new File(codeSource.getLocation().toURI());
      } catch (URISyntaxException e) {
        throw new DatasetException(
            "Cannot locate " + requiredClass.getName() + " jar", e);
      }
    } else {
      // this should only happen for system classes
      throw new DatasetException(
          "Cannot locate " + requiredClass.getName() + " jar");
    }
  }

  /**
   * A PrivilegedAction that gets the ProtectionDomain for a dependency class.
   *
   * Using a PrivilegedAction to retrieve the domain allows security policies
   * to enable Kite to do this, but exclude client code.
   */
  private static class GetProtectionDomain
      implements PrivilegedAction<ProtectionDomain> {
    private final Class<?> requiredClass;

    public GetProtectionDomain(Class<?> requiredClass) {
      this.requiredClass = requiredClass;
    }

    @Override
    public ProtectionDomain run() {
      return requiredClass.getProtectionDomain();
    }
  }
}
