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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import org.kitesdk.data.DatasetDescriptor;
import org.slf4j.Logger;

@Parameters(commandDescription = "Build a schema from a java class")
public class ObjectSchemaCommand extends BaseCommand {

  private final Logger console;

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

    ClassLoader loader = loaderFor(jars, libs);

    String className = classNames.get(0);
    Class<?> recordClass;
    try {
      recordClass = loader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Cannot find class: " + className, e);
    }

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(recordClass)
        .build();
    String schema = descriptor.getSchema().toString(!minimize);

    output(schema, console, outputPath);

    return 0;
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

}
