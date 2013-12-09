/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.maven.plugins;

import java.util.Properties;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

/**
 * Run an app as a job on a cluster.
 */
@Mojo(name = "run-app")
public class RunAppMojo extends AbstractAppMojo {

  /**
   * The URL of the Oozie service to use.
   */
  @Parameter(property = "kite.oozieUrl", required = true)
  private String oozieUrl;

  /**
   * Hadoop configuration properties.
   */
  @Parameter(property = "kite.hadoopConfiguration")
  private Properties hadoopConfiguration;

  /**
   * The type of the application (<code>workflow</code>, <code>coordination</code>,
   * or <code>bundle</code>).
   */
  // TODO: support applications which are more than one type
  @Parameter(property = "kite.applicationType",
      defaultValue = "workflow")
  private String applicationType;

  /**
   * Job configuration properties for the application. This provides a means
   * to specify values for parameterized properties in Oozie applications.
   */
  @Parameter(property = "kite.jobProperties")
  private Properties jobProperties;

  public void execute() throws MojoExecutionException, MojoFailureException {
    OozieClient oozieClient = new OozieClient(oozieUrl);
    Properties conf = oozieClient.createConfiguration();
    if (jobProperties != null) {
      conf.putAll(jobProperties);
    }
    if (hadoopConfiguration != null) {
      conf.putAll(hadoopConfiguration);
      String hadoopFs = hadoopConfiguration.getProperty("fs.default.name");
      if (hadoopFs == null) {
        throw new MojoExecutionException("Missing property 'fs.default.name' in " +
            "hadoopConfiguration");
      }
      String hadoopJobTracker = hadoopConfiguration.getProperty("mapred.job.tracker");
      if (hadoopJobTracker == null) {
        throw new MojoExecutionException("Missing property 'mapred.job.tracker' in " +
            "hadoopConfiguration");
      }
      conf.put(NAMENODE_PROPERTY, hadoopFs);
      conf.put(JOBTRACKER_PROPERTY, hadoopJobTracker);
    }

    String appPath = getAppPath().toString();
    conf.setProperty(getAppPathPropertyName(), appPath);
    conf.setProperty(APP_PATH_PROPERTY, appPath); // used in coordinator.xml
    getLog().info("App path: " + appPath);
    try {
      String jobId = oozieClient.run(conf);
      getLog().info("Running Oozie job " + jobId);
    } catch (OozieClientException e) {
      throw new MojoExecutionException("Error running Oozie job", e);
    }
  }

  private String getAppPathPropertyName() {
    if ("coordinator".equals(applicationType)) {
      return OozieClient.COORDINATOR_APP_PATH;
    } else if ("bundle".equals(applicationType)) {
      return OozieClient.BUNDLE_APP_PATH;
    } else {
      return OozieClient.APP_PATH;
    }
  }
}
