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
package org.kitesdk.data;

import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;
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
 *
 * @since 0.8.0
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
  static void register(
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
   *                                  valid {@link java.net.URI}.
   */
  public static DatasetRepository open(String uri) {
    // uses of URI.create throw IllegalArgumentException if the URI is invalid
    return open(URI.create(uri));
  }

  /**
   * <p>
   * Open a {@link DatasetRepository} for the given URI.
   * </p>
   * <p>
   * This method provides a way to connect to a {@link DatasetRepository}
   * while providing configuration options. For almost all cases, this
   * is the preferred method for retrieving an instance of a 
   * {@link DatasetRepository}.
   * </p>
   * <p>
   * The format of a repository URI is as follows.
   * </p>
   * <code>repo:[storage component]</code>
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
   * form can be used. Alternatively, the authority section can be omitted
   * entirely (e.g. <code>file:/my/path</code>). Either way, it is illegal to
   * provide an authority (i.e.
   * <code>file://this-part-is-illegal/my/path</code>). This storage backend
   * produces a {@link DatasetRepository} that stores both data and metadata
   * on the local operating system filesystem. See
   * {@link FileSystemDatasetRepository} for more information.
   * </p>
   * <h1>HDFS FileSystem URIs</h1>
   * <p>
   * <code>hdfs://[host]:[port]/[path]</code> where <code>[host]</code> and
   * <code>[port]</code> indicate the location of the Hadoop NameNode, and
   * <code>[path]</code> is the dataset repository root directory in which to
   * store dataset data. This form loads the Hadoop configuration
   * information per the usual methods (that is, searching the process's 
   * classpath for the various configuration files). This storage backend 
   * produces a {@link DatasetRepository} that stores both data and metadata in
   * HDFS. See {@link FileSystemDatasetRepository} for more information.
   * </p>
   * <h1>Hive/HCatalog URIs</h1>
   * <p>
   * <code>hive</code> and
   * <code>hive://[metastore-host]:[metastore-port]/</code> connects to the
   * Hive MetaStore.  Dataset locations are determined by Hive as managed
   * tables.
   * </p>
   * <p>
   * <code>hive:/[path]</code> and
   * <code>hive://[metastore-host]:[metastore-port]/[path]</code> also
   * connect to the Hive MetaStore, but tables are external and stored
   * under <code>[path]</code>. The repository storage layout is the same
   * as <code>hdfs</code> and <code>file</code> repositories. HDFS connection
   * options can be supplied by adding <code>hdfs-host</code> and
   * <code>hdfs-port</code> query options to the URI (see examples).
   * </p>
   * <h1>HBase URIs</h1>
   * <p>
   * <code>repo:hbase:[zookeeper-host1]:[zk-port],[zookeeper-host2],...
   * </code> opens an HBase-backed DatasetRepository. This URI can also be
   * instantiated with {@link #openRandomAccess(URI)} to instantiate a {@link
   * RandomAccessDatasetRepository}
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
   * <td><code>repo:hive://meta-host:9083/</code></td>
   * <td>Connects to the Hive MetaStore at <code>thrift://meta-host:9083</code>,
   * and creates managed tables. This only matches when the path is
   * <code>/</code></td>. Any non-root path matches the external Hive URIs.
   * </tr>
   * <tr>
   * <td><code>repo:hive:/path?hdfs-host=localhost&hdfs-port=8020</code></td>
   * <td>Connects to the default Hive MetaStore and creates external tables
   * stored in <code>hdfs://localhost:8020/</code> at <code>path</code>.
   * <code>hdfs-host</code> and <code>hdfs-port</code> are optional.
   * </td>
   * </tr>
   * <tr>
   * <td>
   * <code>repo:hive://meta-host:9083/path?hdfs-host=localhost&amp;hdfs-port=8020
   * </code>
   * </td>
   * <td>
   * Connects to the Hive MetaStore at <code>thrift://meta-host:9083/</code>
   * and creates external tables stored in <code>hdfs://localhost:8020/</code>
   * at <code>path</code>. <code>hdfs-host</code> and <code>hdfs-port</code>
   * are optional.
   * </td>
   * </tr>
   * <tr>
   * <td>
   * <code>repo:hbase:zk1,zk2,zk3</code>
   * </td>
   * <td>
   * Connects to HBase via the given Zookeeper quorum nodes.
   * </td>
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

  /**
   * Synonym for {@link #openRandomAccess(java.net.URI)} for String URIs.
   * 
   * @param uri a String URI
   * @return An appropriate implementation of {@link RandomAccessDatasetRepository}
   * @throws IllegalArgumentException If the String cannot be parsed into a
   *                                  valid {@link java.net.URI}.
   * @since 0.9.0
   */
  public static RandomAccessDatasetRepository openRandomAccess(String uri) {
    return openRandomAccess(URI.create(uri));
  }
  
  /**
   * <p>
   * Synonym for {@link #open(java.net.URI)} for
   * {@link RandomAccessDatasetRepository}s
   * </p>
   * <p>
   * This method provides a way to connect to a {@link DatasetRepository} the 
   * same way {@link #open(java.net.URI)} does, but instead returns an
   * implementation of type {@link RandomAccessDatasetRepository}.
   * You should use this method when you need to access
   * {@link RandomAccessDataset}s to take advantage of the random access methods.
   * </p>
   * </>
   * The format of a repository URI is as follows.
   * </p>
   * <code>repo:[storage component]</code>
   * <p>
   * The <code>[storage component]</code> indicates the underlying metadata and,
   * in some cases, physical storage of the data, along with any options. The
   * supported storage backends are:
   * </p>
   * <h1>HBase URIs</h1>
   * <p>
   * <code>repo:hbase:[zookeeper-host1]:[zk-port],[zookeeper-host2],...
   * </code> will open a HBase-backed DatasetRepository. This URI can also be
   * instantiated with {@link #openRandomAccess(URI)} to instantiate a {@link
   * RandomAccessDatasetRepository}
   * </p>
   * <h1>Examples</h1>
   * <p>
   * <table>
   * </tr>
   * <tr>
   * <td>
   * <code>repo:hbase:zk1,zk2,zk3
   * </code>
   * </td>
   * <td>
   * Connects to HBase via the given Zookeeper quorum nodes.
   * </td>
   * </tr>
   * </table>
   * 
   * @param repositoryUri The repository URI
   * @return An appropriate implementation of {@link RandomAccessDatasetRepository}
   * @since 0.9.0
   */
  public static RandomAccessDatasetRepository openRandomAccess(URI repositoryUri) {
    return (RandomAccessDatasetRepository)open(repositoryUri);
  }
}
