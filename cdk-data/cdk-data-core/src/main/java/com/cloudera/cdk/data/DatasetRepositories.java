/*
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
package com.cloudera.cdk.data;

import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <p>Convenience methods for working with {@link DatasetRepository} instances.</p>
 */
public class DatasetRepositories {

  private static final Logger logger = LoggerFactory.getLogger(DatasetRepositories.class);

  /**
   * <p>
   * Connect to a {@link }DatasetRepository} given a URI.
   * </p>
   * <p>
   * This method provides a simpler way to connect to a {@link DatasetRepository}
   * while providing information about the appropriate {@link MetadataProvider}
   * and other options to use. For almost all cases, this is the preferred method
   * of retrieving an instance of a {@link DatasetRepository}.
   * </p>
   * <p>
   * The format of a repository URI is as follows.
   * </p>
   * <code>dsr:[storage component]</code>
   * <p>
   * The <code>[storage component]</code> indicates the underlying metadata and,
   * in some cases, physical storage of the data, along with any options. The
   * supported storage backends are:
   * </p>
   * <h1>Local FileSystem URIs</h1>
   * <p>
   * <code>file:[path]</code> where <code>[path]</code> is a relative or absolute
   * filesystem path to be used as the dataset repository root directory in which
   * to store dataset data. When specifying an absolute path, the
   * <q>null authority</q> (i.e. <code>file:///my/path</code>)
   * form may be used. Alternatively, the authority section may be omitted
   * entirely (e.g. <code>file:/my/path</code>). Either way, it is illegal to
   * provide an authority (i.e.
   * <code>file://this-part-is-illegal/my/path</code>). This storage backend
   * will produce a {@link DatasetRepository} that stores both data and metadata
   * on the local operating system filesystem. See
   * {@link FileSystemDatasetRepository} for more information.
   * </p>
   * <h1>HDFS FileSystem URIs</h1>
   * <p>
   * <code>hdfs://[host][port]/[path]</code> where <code>[host]</code> and
   * <code>[port]</code> indicate the location of the Hadoop NameNode, and
   * <code>[path]</code> is the dataset repository root directory in which to
   * store dataset data. This form will load the Hadoop configuration
   * information per the usual methods (i.e. searching the process's classpath
   * for the various configuration files) but overrides any value of
   * <code>fs.defaultFS</code> with what is provided. This is to ensure
   * deterministic behaviour and avoid confusion where the URI and on-disk
   * configuration values for <code>fs.defaultFS</code> differ. This storage
   * backend will produce a {@link DatasetRepository} that stores both data and
   * metadata in HDFS. See {@link FileSystemDatasetRepository} for more
   * information.
   * </p>
   * <h1>Hive/HCatalog URIs</h1>
   * <p>
   * Connecting to Hive/HCatalog-backed dataset repositories is not currently
   * supported.
   * </p>
   * <h1>Examples</h1>
   * <p>
   * <table>
   * <tr>
   * <td><code>dsr:file:foo/bar</code></td>
   * <td>Store data+metadata on the local filesystem in the directory
   * <code>./foo/bar</code>.</td>
   * </tr>
   * <tr>
   * <td><code>dsr:file:///data</code></td>
   * <td>Store data+metadata on the local filesystem in the directory
   * <code>/data</code></td>
   * </tr>
   * <tr>
   * <td><code>dsr:hdfs://localhost:8020/data</code></td>
   * <td>Same as above, but stores data+metadata on HDFS.</td>
   * </tr>
   * </table>
   * </p>
   *
   * @param repositoryUri The repository URI
   * @return An appropriate implementation of {@link DatasetRepository}
   * @since 0.8.0
   */
  public static DatasetRepository connect(URI repositoryUri) {
    String scheme = repositoryUri.getScheme();
    String schemeSpecific = repositoryUri.getSchemeSpecificPart();

    Preconditions.checkArgument(scheme != null && scheme.equals("dsr"),
      "Invalid dataset repository URI:%s - scheme must be `dsr:`", repositoryUri);
    Preconditions.checkArgument(schemeSpecific != null,
      "Invalid dataset repository URI:%s - missing storage component", repositoryUri);

    DatasetRepository repo = null;
    URI uriInternal = null;

    try {
      uriInternal = new URI(schemeSpecific);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri
        + " - storage component:" + schemeSpecific + " is malformed - " + e.getMessage());
    }

    Preconditions.checkArgument(uriInternal.getScheme() != null,
      "Invalid dataset repository URI:%s - storage component doesn't contain a valid scheme:%s",
      repositoryUri, schemeSpecific);

    if (uriInternal.getScheme().equals("file")) {
      Configuration conf = new Configuration();

      // TODO: Support non-2.0 versions.
      conf.set("fs.defaultFS", "file:///");

      Path basePath = null;

      // A URI's path can be null if it's relative. e.g. file:foo/bar.
      if (uriInternal.getPath() != null) {
        basePath = new Path(uriInternal.getPath());
      } else if (uriInternal.getSchemeSpecificPart() != null) {
        basePath = new Path(uriInternal.getSchemeSpecificPart());
      } else {
        throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri
          + " - storage component:" + schemeSpecific + " doesn't seem to have a path");
      }

      try {
        repo = new FileSystemDatasetRepository(FileSystem.get(conf), basePath);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load Hadoop FileSystem implementation - " + e.getMessage(), e);
      }
    } else if (uriInternal.getScheme().equals("hdfs")) {
      Configuration conf = new Configuration();

      // TODO: Support non-2.0 versions.
      conf.set("fs.defaultFS", "hdfs://" + uriInternal.getAuthority() + "/");

      try {
        repo = new FileSystemDatasetRepository(FileSystem.get(conf), new Path(uriInternal.getPath()));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load Hadoop FileSystem implementation - " + e.getMessage(), e);
      }
    } else if (uriInternal.getScheme().equals("hive")) {
      throw new UnsupportedOperationException("Hive/HCatalog-based datasets are not yet supported. URI:" + repositoryUri);
    } else {
      throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri + " - unsupported storage method:" + schemeSpecific);
    }

    logger.debug("Connected to repository:{} using uri:{}", repo, repositoryUri);

    return repo;
  }
}