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
package org.kitesdk.data.spi.filesystem;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.spi.SchemaValidationUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * Manager for creating, updating, and using Avro schemas stored in a
 * filesystem, typically HDFS.
 */
public class SchemaManager {

  private final Path schemaDirectory;
  private final FileSystem rootFileSystem;

  /**
   * Creates a new schema manager using the given root directory of a
   * dataset for its base.
   *
   * @param conf the Hadoop configuration
   * @param schemaDirectory directory in which the manager
   *                        stores schemas.
   *
   * @return a schema manager instance.
   */
  public static SchemaManager create(Configuration conf, Path schemaDirectory) {

    try {
      FileSystem rootFileSystem = schemaDirectory.getFileSystem(conf);

      rootFileSystem.mkdirs(schemaDirectory);

      return new SchemaManager(schemaDirectory, rootFileSystem);
    } catch (IOException e) {
      throw new DatasetIOException("Unable to create schema manager directory: "
              + schemaDirectory, e);
    }

  }

  /**
   * Loads a schema manager that stores data under the given dataset root
   * directory it exists. Returns <code>null</code> if it does not.
   *
   * @param conf the Hadoop configuration
   * @param schemaDirectory directory in which the manager stores schemas.
   *
   * @return a schema manager instance, or <code>null</code> if the given
   * directory does not exist.
   */
  public static SchemaManager load(Configuration conf, Path schemaDirectory) {

    try {

      FileSystem rootFileSystem = schemaDirectory.getFileSystem(conf);

      if (rootFileSystem.exists(schemaDirectory)) {
        return new SchemaManager(schemaDirectory, rootFileSystem);
      } else {
        return null;
      }

    } catch (IOException e) {
      throw new DatasetIOException ("Cannot load schema manager at:"
              + schemaDirectory, e);
    }
  }

  private SchemaManager(Path schemaDirectory, FileSystem rootFileSystem)
          throws IOException  {
    this.schemaDirectory = schemaDirectory;
    this.rootFileSystem = rootFileSystem;
  }

  /**
   * Simple return the number used for the name of the file.
   */
  private static int getFileNumber(FileStatus fileStatus) {

    try {

      String fileName = fileStatus.getPath().getName();

      return Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));

    } catch (NumberFormatException e) {
      throw new DatasetException("Unexpected file in schema manager folder " +
              fileStatus.getPath(), e);
    }
  }

  /**
   * Comparator to identify the newest schema file.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
          value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
          justification="Implement if we intend to use in Serializable objects "
                  + " (e.g., TreeMaps) and use java serialization.")
  private static final class FileNameComparator
          implements Comparator<FileStatus> {

    @Override
    public int compare(FileStatus file1, FileStatus file2) {

      return getFileNumber(file1) - getFileNumber(file2);
    }
  }

  /**
   * Returns the path of the newest schema file, or null if none exists.
   */
  private Path newestFile() {

    try {
      FileStatus[] statuses = rootFileSystem.listStatus(schemaDirectory);

      // No schema files exist, so return null;
      if (statuses.length == 0) {
        return null;
      }

      // Sort the schema files and return the newest one.
      Arrays.sort(statuses, new FileNameComparator());

      return statuses[statuses.length - 1].getPath();

    } catch (IOException e) {
      throw new DatasetIOException("Unable to list schema files.", e);
    }
  }

  /**
   * Returns the URI of the newest schema in the manager.
   */
  public URI getNewestSchemaURI() {

    Path path = newestFile();

    return path == null ? null : rootFileSystem.makeQualified(path).toUri();
  }

  private Schema loadSchema(Path schemaPath) {
    Schema schema = null;
    InputStream inputStream = null;
    boolean threw = true;

    try {
      inputStream = rootFileSystem.open(schemaPath);
      schema = new Schema.Parser().parse(inputStream);
      threw = false;
    } catch (IOException e) {
      throw new DatasetIOException(
              "Unable to load schema file:" + schemaPath, e);
    } finally {
      try {
        Closeables.close(inputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    return schema;
  }

  /**
   * Gets the newest schema version being managed.
   *
   * @return thew newest schema version
   */
  public Schema getNewestSchema() {
    Path schemaPath = newestFile();

    return schemaPath == null ? null : loadSchema(schemaPath);
  }

  /**
   * Imports an existing schema stored at the given path. This
   * is generally used to bring in schemas written by previous
   * versions of this library.
   *
   * @param schemaPath A path to a schema to import
   * @return The URI of the schema file managed by this manager.
   */
  public URI importSchema(Path schemaPath) {

    Schema schema = loadSchema(schemaPath);

    return writeSchema(schema);
  }

  /**
   * Writes the schema and a URI to that schema and returns a URI to the
   * schema file it writes. The URI can be used in the Hive metastore or
   * other tools.
   *
   * @param schema the schema to write
   * @return A URI pointing to the written schema
   */
  public URI writeSchema(Schema schema) {

    Path previousPath = newestFile();

    // If the previous schema is identical to the current update,
    // simply keep the previous one.
    if (previousPath != null) {

      Schema previousSchema = loadSchema(previousPath);

      if (schema.equals(previousSchema)) {

        return rootFileSystem.makeQualified(previousPath).toUri();
      }
    }

    // Ensure all previous schemas are compatible with the new one.
    // This is necessary because with Avro schema evolution,
    // it is possible for all schemas to be compatible with their
    // immediate predecessor but not with a further ancestor.
    Map<Integer, Schema> schemas = getSchemas();

    for (Schema oldSchema: schemas.values()) {
      if (!SchemaValidationUtil.canRead(oldSchema, schema)) {
        throw new IncompatibleSchemaException("Schema cannot read data " +
                "written using existing schema. Schema: " +
                schema.toString(true) +
                "\nPrevious schema: " + oldSchema.toString(true));
      }
    }

    Path schemaPath = null;

    if (previousPath == null) {
      schemaPath = new Path(schemaDirectory, "1.avsc");
    } else {

      String previousName = previousPath.getName()
              .substring(0, previousPath.getName().indexOf('.'));

      int i = Integer.parseInt(previousName) + 1;

      schemaPath = new Path(schemaDirectory, Integer.toString(i) + ".avsc");
    }

    FSDataOutputStream outputStream = null;
    boolean threw = true;
    try {
      outputStream = rootFileSystem.create(schemaPath, false);
      outputStream.write(schema.toString(true)
              .getBytes(Charsets.UTF_8));
      outputStream.flush();
      threw = false;
    } catch (IOException e) {
      throw new DatasetIOException(
              "Unable to save schema file: " + schemaPath, e);
    } finally {
      try {
        Closeables.close(outputStream, threw);
      } catch (IOException e) {
        throw new DatasetIOException("Cannot close", e);
      }
    }

    return rootFileSystem.makeQualified(schemaPath).toUri();
  }

  /**
   * Returns a map of schema versions with the schemas themselves.
   */
  public Map<Integer,Schema> getSchemas() {

    Map<Integer,Schema> schemas = new TreeMap<Integer, Schema>();

    try {
      FileStatus[] statuses = rootFileSystem.listStatus(schemaDirectory);

      for (FileStatus fileStatus: statuses) {

        int schemaNumber = getFileNumber(fileStatus);

        Schema schema = loadSchema(fileStatus.getPath());

        schemas.put(schemaNumber, schema);
      }

    } catch (IOException e) {
      throw new DatasetIOException("Unable to list schema files.", e);
    }

    return schemas;
  }
}
