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

import com.google.common.annotations.VisibleForTesting;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.DatasetRepository;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DefaultConfiguration;

abstract class AbstractDatasetMojo extends AbstractHadoopMojo {

  /**
   * A Kite dataset URI.
   */
  @Parameter(property = "kite.uri")
  protected String uri;

  /**
   * The root directory of the dataset repository. Optional if using HCatalog for metadata storage.
   */
  @Parameter(property = "kite.rootDirectory")
  protected String rootDirectory;

  /**
   * If true, store dataset metadata in HCatalog, otherwise store it on the filesystem.
   */
  @Parameter(property = "kite.hcatalog")
  protected boolean hcatalog = true;

  /**
   * The URI specifying the dataset repository, e.g. <i>repo:hdfs://host:8020/data</i>.
   * Optional, but if specified then <code>kite.rootDirectory</code> and
   * <code>kite.hcatalog</code> are ignored.
   */
  @Parameter(property = "kite.repositoryUri")
  protected String repositoryUri;

  /**
   * Hadoop configuration properties.
   */
  @VisibleForTesting
  @Parameter(property = "kite.hadoopConfiguration")
  protected Properties hadoopConfiguration;

  private static void addToConfiguration(Properties hadoopConfiguration) {
    // base the new Configuration on the current defaults
    Configuration conf = new Configuration(DefaultConfiguration.get());

    // add all of the properties as config settings
    for (String key : hadoopConfiguration.stringPropertyNames()) {
      String value = hadoopConfiguration.getProperty(key);
      conf.set(key, value);
    }

    // replace the original Configuration
    DefaultConfiguration.set(conf);

    addedConf = true;
  }

  @VisibleForTesting
  static boolean addedConf = false;

  protected Configuration getConf() {
    if (!addedConf) {
      addToConfiguration(hadoopConfiguration);
    }
    // use the default
    return DefaultConfiguration.get();
  }

  DatasetRepository getDatasetRepository() {
    getConf(); // ensure properties are added to DefaultConfig

    if (repositoryUri != null) {
      return DatasetRepositories.repositoryFor(repositoryUri);
    }

    if (rootDirectory != null) {
      URI uri = URI.create(rootDirectory);
      if (hcatalog) {
        return DatasetRepositories.repositoryFor("repo:hive:" + uri.getPath());
      } else if (uri.getScheme() != null) {
        return DatasetRepositories.repositoryFor("repo:" + uri.toString());
      } else if (getConf().get("fs.defaultFS") != null) {
        URI defaultFS = URI.create(getConf().get("fs.defaultFS"));
        return DatasetRepositories.repositoryFor(
            "repo:" + defaultFS.getScheme() + ":" + uri.getPath());
      } else if (getConf().get("fs.default.name") != null) {
        URI defaultFS = URI.create(getConf().get("fs.default.name"));
        return DatasetRepositories.repositoryFor(
            "repo:" + defaultFS.getScheme() + ":" + uri.getPath());
      } else {
        return DatasetRepositories.repositoryFor("repo:file:" + uri.getPath());
      }
    } else if (hcatalog) {
      return DatasetRepositories.repositoryFor("repo:hive");
    }
    throw new IllegalArgumentException(
        "Root directory must be specified if not using Hive.");
  }

  void configureSchema(DatasetDescriptor.Builder descriptorBuilder, String
      avroSchemaFile, String avroSchemaReflectClass) throws MojoExecutionException {
    if (avroSchemaFile != null) {
      File avroSchema = new File(avroSchemaFile);
      try {
        if (avroSchema.exists()) {
          descriptorBuilder.schema(avroSchema);
        } else {
          descriptorBuilder.schema(Resources.getResource(avroSchemaFile).openStream());
        }
      } catch (IOException e) {
        throw new MojoExecutionException("Problem while reading file " + avroSchemaFile, e);
      }
    } else if (avroSchemaReflectClass != null) {

      try {
        List<URL> classpath = new ArrayList<URL>();
        for (Object element : mavenProject.getCompileClasspathElements()) {
          String path = (String) element;
          classpath.add(new File(path).toURI().toURL());
        }
        ClassLoader parentClassLoader = getClass().getClassLoader(); // use Maven's classloader, not the system one
        ClassLoader classLoader = new URLClassLoader(
            classpath.toArray(new URL[classpath.size()]), parentClassLoader);

        descriptorBuilder.schema(Class.forName(avroSchemaReflectClass, true, classLoader));
      } catch (ClassNotFoundException e) {
        throw new MojoExecutionException("Problem finding class " +
            avroSchemaReflectClass, e);
      } catch (MalformedURLException e) {
        throw new MojoExecutionException("Problem finding class " +
            avroSchemaReflectClass, e);
      } catch (DependencyResolutionRequiredException e) {
        throw new MojoExecutionException("Problem finding class " +
            avroSchemaReflectClass, e);
      }
    }
  }

}
