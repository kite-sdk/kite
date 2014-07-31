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

import com.google.common.base.Preconditions;
import java.net.URI;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;

/**
 * <p>Convenience methods for working with {@link DatasetRepository} instances.</p>
 *
 * @since 0.8.0
 *
 * @deprecated will be removed in 0.17.0. Move to using {@link Datasets} instead
 */
@Deprecated
public class DatasetRepositories {

  private static final String REPO_SCHEME = "repo";

  /**
   * Synonym for {@link #open(java.net.URI)} for String URIs.
   *
   * @param uri a String URI
   * @return a DatasetRepository for the given URI.
   * @throws IllegalArgumentException If the String cannot be parsed into a
   *                                  valid {@link java.net.URI}.
   *
   * @deprecated will be removed in 0.17.0. Move to using
   * {@link Datasets#load(java.lang.String, java.lang.Class)} instead
   */
  @Deprecated
  public static DatasetRepository open(String uri) {
    // uses of URI.create throw IllegalArgumentException if the URI is invalid
    return open(URI.create(uri));
  }

  /**
   * <p>
   * Open a {@link DatasetRepository} for the given URI.
   * </p>
   * <p>
   * This method provides a way to open to a {@link DatasetRepository}
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
   * <code>hive://[metastore-host]:[metastore-port]</code> connects to the
   * Hive MetaStore.  Dataset locations are determined by Hive as managed
   * tables.
   * </p>
   * <p>
   * <code>hive:/[path]</code> and
   * <code>hive://[metastore-host]:[metastore-port]/[path]</code> also
   * connect to the Hive MetaStore, but tables are external and stored
   * under <code>[path]</code>. The repository storage layout is the same
   * as <code>hdfs</code> and <code>file</code> repositories. HDFS connection
   * options can be supplied by adding <code>hdfs:host</code> and
   * <code>hdfs:port</code> query options to the URI (see examples).
   * </p>
   * <h1>HBase URIs</h1>
   * <p>
   * <code>repo:hbase:[zookeeper-host1]:[zk-port],[zookeeper-host2],...
   * </code> opens an HBase-backed DatasetRepository. This URI will return a
   * {@link RandomAccessDatasetRepository}
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
   * <td><code>repo:hive://meta-host:9083</code></td>
   * <td>Connects to the Hive MetaStore at <code>thrift://meta-host:9083</code>,
   * and creates managed tables.
   * </tr>
   * <tr>
   * <td><code>repo:hive:/path?hdfs:host=localhost&hdfs:port=8020</code></td>
   * <td>Connects to the default Hive MetaStore and creates external tables
   * stored in <code>hdfs://localhost:8020/</code> at <code>path</code>.
   * <code>hdfs:host</code> and <code>hdfs:port</code> are optional.
   * </td>
   * </tr>
   * <tr>
   * <td>
   * <code>repo:hive://meta-host:9083/path?hdfs:host=localhost&amp;hdfs:port=8020
   * </code>
   * </td>
   * <td>
   * Connects to the Hive MetaStore at <code>thrift://meta-host:9083/</code>
   * and creates external tables stored in <code>hdfs://localhost:8020/</code>
   * at <code>path</code>. <code>hdfs:host</code> and <code>hdfs:port</code>
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
   * @param repoUri The repository URI
   * @return An appropriate implementation of {@link DatasetRepository}
   * @since 0.8.0
   *
   * @deprecated will be removed in 0.17.0. Move to using
   * {@link Datasets#load(java.net.URI, java.lang.Class)} instead
   */
  @Deprecated
  public static DatasetRepository open(URI repoUri) {
    Preconditions.checkArgument(REPO_SCHEME.equals(repoUri.getScheme()),
        "Not a repository URI: " + repoUri);
    return Registration.open(URI.create(repoUri.getRawSchemeSpecificPart()));
  }

  /**
   * Synonym for {@link #openRandomAccess(java.net.URI)} for String URIs.
   *
   * @param uri a String URI
   * @return An appropriate implementation of {@link RandomAccessDatasetRepository}
   * @throws IllegalArgumentException If the String cannot be parsed into a
   *                                  valid {@link java.net.URI}.
   * @since 0.9.0
   *
   * @deprecated will be removed in 0.17.0. Move to using
   * {@link Datasets#load(java.lang.String, java.lang.Class)} instead
   */
  @Deprecated
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
   * You should use this method when you need to access a
   * {@link RandomAccessDataset} to use random access methods, such as
   * {@link RandomAccessDataset#put(Object)}.
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
   *
   * @deprecated will be removed in 0.17.0. Move to using
   * {@link Datasets#load(java.net.URI, java.lang.Class)} instead
   */
  @Deprecated
  public static RandomAccessDatasetRepository openRandomAccess(URI repositoryUri) {
    return (RandomAccessDatasetRepository) open(repositoryUri);
  }
}
