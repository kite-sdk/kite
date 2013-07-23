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
package com.cloudera.cdk.maven.plugins;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Deploy a packaged Oozie application to a Hadoop filesystem, such as HDFS.
 */
@Mojo(name = "deploy-app")
public class DeployOozieMojo extends AbstractOozieMojo {

  /**
   * The local directory of the Oozie application to deploy.
   */
  @Parameter(property = "cdk.localApplicationFile",
      defaultValue = "${project.build.directory}/${project.build.finalName}-oozie-app")
  private File localApplicationFile;

  /**
   * The Hadoop fileystem used to deploy the Oozie application. The filesystem must be
   * accessible by the client deploying the application. For this reason, WebHDFS or
   * HttpFS are common choices (both use the webhdfs:// protocol).
   */
  @Parameter(property = "cdk.deployFileSystem", required = true)
  private String deployFileSystem;

  /**
   * Whether to automatically undeploy applications that already exist when deploying.
   */
  @Parameter(property = "cdk.updateApplication", defaultValue = "false")
  private boolean updateApplication;

  public void execute() throws MojoExecutionException, MojoFailureException {
    try {
      Configuration conf = new Configuration();
      Path appPath = getAppPath();
      getLog().info("Deploying " + localApplicationFile +  " to " +  appPath);

      FileSystem destFileSystem = FileSystem.get(new URI(deployFileSystem), conf);
      if (destFileSystem.exists(appPath)) {
        if (!updateApplication) {
          throw new MojoExecutionException("Oozie application already exists at " +
              appPath + ". Use 'updateApplication' option to force deployment.");
        }
        boolean success = destFileSystem.delete(appPath, true);
        if (!success) {
          throw new MojoExecutionException("Error deleting existing Oozie application " +
              "at " + appPath);
        }
      }
      boolean success = FileUtil.copy(localApplicationFile, destFileSystem, appPath, false, conf);
      if (!success) {
        throw new MojoExecutionException("Error creating parent directories " +
            "for deploying Oozie application");
      }
    } catch (URISyntaxException e) {
      throw new MojoExecutionException("Syntax error in 'deployFileSystem': "
          + deployFileSystem, e);
    } catch (IOException e) {
      throw new MojoExecutionException("Error deploying Oozie application", e);
    }
  }
}
