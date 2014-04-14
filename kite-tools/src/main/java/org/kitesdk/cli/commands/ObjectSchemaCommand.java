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

package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.cli.Command;
import org.slf4j.Logger;

@Parameters(commandDescription = "Build a schema from a java class")
public class ObjectSchemaCommand implements Configurable, Command {

  private static final Charset SCHEMA_CHARSET = Charset.forName("utf8");
  private final Logger console;
  private Configuration conf;

  public ObjectSchemaCommand(Logger console) {
    this.console = console;
  }

  @Parameter(description="<class name>")
  List<String> classNames;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UWF_NULL_FIELD",
      justification = "Field set by JCommander")
  @Parameter(names={"-o", "--output"}, description="Save schema avsc to path")
  String outputPath = null;

  @Parameter(names="--minimize",
      description="Minimize schema file size by eliminating white space")
  boolean minimize=false;

  @Parameter(names="--jar",
      description="Add a jar to the classpath used when loading the java class")
  List<String> jars;

  @Parameter(names="--lib-dir",
      description="Add a directory to the classpath used when loading the java class")
  List<String> libs;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(classNames != null && !classNames.isEmpty(),
        "Java class name is required");
    Preconditions.checkArgument(classNames.size() == 1,
        "Only one java class name can be given");

    // check the additional jars and lib directories in the local FS
    final List<URL> urls = Lists.newArrayList();
    if (libs != null) {
      for (String lib : libs) {
        // final URLs must end in '/' for URLClassLoader
        File path = lib.endsWith("/") ? new File(lib) : new File(lib + "/");
        Preconditions.checkArgument(path.exists(),
            "Lib directory does not exist: " + lib);
        Preconditions.checkArgument(path.isDirectory(),
            "Not a directory: " + lib);
        Preconditions.checkArgument(path.canRead() && path.canExecute(),
            "Insufficient permissions to access lib directory: " + lib);
        urls.add(path.toURI().toURL());
      }
    }
    if (jars != null) {
      for (String jar : jars) {
        File path = new File(jar);
        Preconditions.checkArgument(path.exists(),
            "Jar files does not exist: " + jar);
        Preconditions.checkArgument(path.isFile(),
            "Not a file: " + jar);
        Preconditions.checkArgument(path.canRead(),
            "Cannot read jar file: " + jar);
        urls.add(path.toURI().toURL());
      }
    }

    ClassLoader loader = AccessController.doPrivileged(
        new GetClassLoader(urls));

    String className = classNames.get(0);
    Class<?> recordClass;
    try {
      recordClass = loader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Cannot find class: " + className, e);
    }

    String schema = ReflectData.get().getSchema(recordClass).toString(!minimize);

    if (outputPath == null || "-".equals(outputPath)) {
      console.info(schema);
    } else {
      Path out = new Path(outputPath);
      FileSystem outFS = out.getFileSystem(conf);
      FSDataOutputStream outgoing = outFS.create(out, true /* overwrite */ );
      try {
        outgoing.write(schema.getBytes(SCHEMA_CHARSET));
      } finally {
        outgoing.close();
      }
    }

    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create a schema for an example User class:",
        "org.kitesdk.cli.example.User",
        "# Create a schema for a class in a jar:",
        "com.example.MyRecord --jar my-application.jar",
        "# Save the schema for the example User class to user.avsc:",
        "org.kitesdk.cli.example.User -o user.avsc"
    );
  }

  public static class GetClassLoader implements PrivilegedAction<ClassLoader> {
    private final URL[] urls;

    public GetClassLoader(List<URL> urls) {
      this.urls = urls.toArray(new URL[urls.size()]);
    }

    @Override
    public ClassLoader run() {
      return new URLClassLoader(
          urls, Thread.currentThread().getContextClassLoader());
    }
  }
}
