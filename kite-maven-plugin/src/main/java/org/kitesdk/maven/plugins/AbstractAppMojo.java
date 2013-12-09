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

import org.apache.hadoop.fs.Path;
import org.apache.maven.plugins.annotations.Parameter;

abstract class AbstractAppMojo extends AbstractHadoopMojo {

  // TODO: namespace?
  public static final String NAMENODE_PROPERTY = "nameNode";
  public static final String JOBTRACKER_PROPERTY = "jobTracker";
  public static final String APP_PATH_PROPERTY = "appPath";

  /**
   * The base directory in the Hadoop filesystem (typically HDFS) where
   * applications are stored.
   */
  @Parameter(property = "kite.applicationsDirectory",
      defaultValue = "/user/${user.name}/apps/")
  protected String applicationsDirectory;

  /**
   * The name of the application.
   */
  @Parameter(property = "kite.applicationName",
      defaultValue = "${project.build.finalName}-app")
  protected String applicationName;

  protected Path getAppPath() {
    return new Path(applicationsDirectory, applicationName);
  }

}
