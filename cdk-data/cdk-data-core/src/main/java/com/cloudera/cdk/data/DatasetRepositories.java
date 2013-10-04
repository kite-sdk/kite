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
import com.cloudera.cdk.data.spi.Loadable;
import com.cloudera.cdk.data.spi.OptionBuilder;
import com.cloudera.cdk.data.spi.URIPattern;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * <p>Convenience methods for working with {@link DatasetRepository} instances.</p>
 */
public class DatasetRepositories {

  private static final Logger logger = LoggerFactory.getLogger(DatasetRepositories.class);

  private static final URIPattern BASE_PATTERN = new URIPattern(
      URI.create("repo:*storage-uri"));
  private static final Map<URIPattern, OptionBuilder<DatasetRepository>>
      REGISTRY = Maps.newLinkedHashMap();

  /**
   * Registers a {@link URIPattern} and an {@link OptionBuilder} to create
   * instances of DatasetRepository from the pattern's match options.
   *
   * @param pattern a URIPattern
   * @param builder an OptionBuilder that expects options defined by
   *                {@code pattern} and builds DatasetRepository instances.
   */
  public static void register(
      URIPattern pattern, OptionBuilder<DatasetRepository> builder) {
    REGISTRY.put(pattern, builder);
  }

  static {
    // load implementations, which will register themselves
    ServiceLoader<Loadable> impls =
        ServiceLoader.load(Loadable.class);
    for (Loadable loader : impls) {
      // the ServiceLoader is lazy, so this iteration forces service loading
      logger.debug("Loading: " + loader.getClass().getName());
      loader.load();
    }
    logger.debug(
        "Registered repository URIs: " +
        Joiner.on(", ").join(REGISTRY.keySet()));
  }

  /**
   * Synonym for {@link #open(java.net.URI)} for String URIs.
   *
   * @param uri a String URI
   * @return a DatasetRepository for the given URI.
   * @throws IllegalArgumentException If the String cannot be parsed into a
   *                                  valid URI ({@see java.net.URI}).
   */
  public static DatasetRepository open(String uri) {
    // uses of URI.create throw IllegalArgumentException if the URI is invalid
    return open(URI.create(uri));
  }

  /**
   * <p>
   * Open a {@link }DatasetRepository} for the given URI.
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
   * <code>hdfs://[host]:[port]/[path]</code> where <code>[host]</code> and
   * <code>[port]</code> indicate the location of the Hadoop NameNode, and
   * <code>[path]</code> is the dataset repository root directory in which to
   * store dataset data. This form will load the Hadoop configuration
   * information per the usual methods (i.e. searching the process's classpath
   * for the various configuration files). This storage backend will produce a
   * {@link DatasetRepository} that stores both data and metadata in HDFS. See
   * {@link FileSystemDatasetRepository} for more information.
   * </p>
   * <h1>Hive/HCatalog URIs</h1>
   * <p>
   * <code>hive</code> will connect to the Hive MetaStore. Dataset locations
   * will be determined by Hive as managed tables.
   * </p>
   * <p>
   * <code>hive:/[path]</code> will also connect to the Hive MetaStore, but
   * tables will be external and stored under <code>[path]</code>. The
   * repository storage layout will be the same as <code>hdfs</code> and
   * <code>file</code> repositories. HDFS connection options can be supplied
   * by adding <code>hdfs-host</code> and <code>hdfs-port</code> query options
   * to the URI (see examples).
   * </p>
   * <h1>Examples</h1>
   * <p>
   * <table>
   * <tr>
   * <td><code>repo:file:foo/bar</code></td>
   * <td>Store data+metadata on the local filesystem in the directory
   * <code>./foo/bar</code>.</td>
   * </tr>
   * <tr>
   * <td><code>repo:file:///data</code></td>
   * <td>Store data+metadata on the local filesystem in the directory
   * <code>/data</code></td>
   * </tr>
   * <tr>
   * <td><code>repo:hdfs://localhost:8020/data</code></td>
   * <td>Same as above, but stores data+metadata on HDFS.</td>
   * </tr>
   * <tr>
   * <td><code>repo:hive</code></td>
   * <td>Connects to the Hive MetaStore and creates managed tables.</td>
   * </tr>
   * <tr>
   * <td><code>repo:hive:/path?hdfs-host=localhost&hdfs-port=8020</code></td>
   * <td>Connects to the Hive MetaStore and creates external tables stored in
   * <code>hdfs://localhost:8020/path</code>.</td>
   * </tr>
   * </table>
   * </p>
   *
   * @param repositoryUri The repository URI
   * @return An appropriate implementation of {@link DatasetRepository}
   * @since 0.8.0
   */
  public static DatasetRepository open(URI repositoryUri) {
    final Map<String, String> baseMatch = BASE_PATTERN.getMatch(repositoryUri);

    Preconditions.checkArgument(baseMatch != null,
        "Invalid dataset repository URI:%s - scheme must be `repo:`",
        repositoryUri);

    final URI storage = URI.create(baseMatch.get("storage-uri"));
    Map<String, String> match;

    for (URIPattern pattern : REGISTRY.keySet()) {
      match = pattern.getMatch(storage);
      if (match != null) {
        final OptionBuilder<DatasetRepository> builder = REGISTRY.get(pattern);
        final DatasetRepository repo = builder.getFromOptions(match);
        logger.debug(
            "Connected to repository:{} using uri:{}", repo, repositoryUri);

        return repo;
      }
    }

    throw new IllegalArgumentException("Unknown storage URI:" + storage);
  }

}