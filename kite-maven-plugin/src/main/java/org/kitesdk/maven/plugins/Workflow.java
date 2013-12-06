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

import java.io.File;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;

class Workflow {
  private File destinationFile;
  private String schemaVersion;
  private String name;
  private String toolClass;
  private String[] args;
  private Properties hadoopConfiguration;
  private String hadoopFs;
  private String hadoopJobTracker;
  private List<Path> libJars;

  public Workflow(File destinationFile, String schemaVersion, String name, String toolClass,
      String[] args, Properties hadoopConfiguration,
      String hadoopFs, String hadoopJobTracker, List<Path> libJars) {
    this.destinationFile = destinationFile;
    this.schemaVersion = schemaVersion;
    this.name = name;
    this.toolClass = toolClass;
    this.args = args;
    this.hadoopConfiguration = hadoopConfiguration;
    this.hadoopFs = hadoopFs;
    this.hadoopJobTracker = hadoopJobTracker;
    this.libJars = libJars;
  }

  public File getDestinationFile() {
    return destinationFile;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public String getName() {
    return name;
  }

  public String getToolClass() {
    return toolClass;
  }

  public String[] getArgs() {
    return args;
  }

  public Properties getHadoopConfiguration() {
    return hadoopConfiguration;
  }

  public String getHadoopFs() {
    return hadoopFs;
  }

  public String getHadoopJobTracker() {
    return hadoopJobTracker;
  }

  public List<Path> getLibJars() {
    return libJars;
  }
}
