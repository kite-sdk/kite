/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.hcatalog.impl;

import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.hcatalog.HCatalogDatasetRepository;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.URIPattern;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Preconditions;

/**
 * A Loader implementation to register URIs for FileSystemDatasetRepositories.
 */
public class Loader implements Loadable {

  private static final Logger logger = LoggerFactory.getLogger(Loader.class);

  public static final String HIVE_METASTORE_URI_PROP = "hive.metastore.uris";
  private static final int UNSPECIFIED_PORT = -1;
  private static final String ALWAYS_REPLACED = "ALWAYS-REPLACED";

  /**
   * This class builds configured instances of
   * {@code FileSystemDatasetRepository} from a Map of options. This is for the
   * URI system.
   */
  private static class ExternalBuilder implements OptionBuilder<DatasetRepository> {

    private final Configuration envConf;

    public ExternalBuilder(Configuration envConf) {
      this.envConf = envConf;
    }

    @Override
    public DatasetRepository getFromOptions(Map<String, String> match) {
      logger.debug("External URI options: {}", match);
      final Path root;
      String path = match.get("path");
      if (match.containsKey("absolute")
          && Boolean.valueOf(match.get("absolute"))) {
        root = path == null ? new Path("/") : new Path("/", path);
      } else {
        root = path == null ? new Path(".") : new Path(path);
      }
      final FileSystem fs;
      try {
        fs = FileSystem.get(fileSystemURI(match), envConf);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(
            "Could not get a FileSystem", ex);
      }

      // make a modifiable copy and setup the MetaStore URI
      Configuration conf = new Configuration(envConf);
      setMetaStoreURI(conf, match);

      return new HCatalogDatasetRepository.Builder()
          .configuration(conf)
          .rootDirectory(fs.makeQualified(root))
          .build();
    }
  }

  private static class ManagedBuilder implements OptionBuilder<DatasetRepository> {
    private final Configuration envConf;

    public ManagedBuilder(Configuration envConf) {
      this.envConf = envConf;
    }

    @Override
    public DatasetRepository getFromOptions(Map<String, String> match) {
      logger.debug("Managed URI options: {}", match);
      // make a modifiable copy and setup the MetaStore URI
      Configuration conf = new Configuration(envConf);
      // sanity check the URI
      Preconditions.checkArgument(!ALWAYS_REPLACED.equals(match.get("host")),
          "[BUG] URI matched but authority was not replaced.");
      setMetaStoreURI(conf, match);
      return new HCatalogDatasetRepository.Builder()
          .configuration(conf)
          .build();
    }
  }

  @Override
  public void load() {
    // get a default Configuration to configure defaults (so it's okay!)
    final Configuration conf = new Configuration();

    String hiveAuthority;
    if (conf.get(HIVE_METASTORE_URI_PROP) != null) {
      try {
        hiveAuthority = new URI(conf.get(HIVE_METASTORE_URI_PROP))
            .getAuthority();
      } catch (URISyntaxException ex) {
        hiveAuthority = "";
      }
    } else {
      hiveAuthority = "";
    }

    // Hive-managed data sets
    final OptionBuilder<DatasetRepository> managedBuilder =
        new ManagedBuilder(conf);
    Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("hive")), managedBuilder);
    // add a URI with no path to allow overriding the metastore authority
    // the authority section is *always* a URI without it cannot match and one
    // with a path (so missing authority) also cannot match
    Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("hive://" + ALWAYS_REPLACED)),
        managedBuilder);

    // external data sets
    final OptionBuilder<DatasetRepository> externalBuilder =
        new ExternalBuilder(conf);

    String hdfsAuthority;
    try {
      // Use a HDFS URI with no authority and the environment's configuration
      // to find the default HDFS information
      final URI hdfs = FileSystem.get(URI.create("hdfs:/"), conf).getUri();
      hdfsAuthority = "&hdfs-host=" + hdfs.getHost() +
          "&hdfs-port=" + hdfs.getPort();
    } catch (IOException ex) {
      logger.warn(
          "Could not locate HDFS, hdfs-host and hdfs-port " +
          "will not be set by default for Hive repositories.");
      hdfsAuthority = "";
    }

    Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("hive://" + hiveAuthority +
            "/*path?absolute=true" + hdfsAuthority)),
        externalBuilder);
    Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("hive:*path")), externalBuilder);
  }

  private static URI fileSystemURI(Map<String, String> match) {
    final String userInfo;
    if (match.containsKey("username")) {
      if (match.containsKey("password")) {
        userInfo = match.get("username") + ":" +
            match.get("password");
      } else {
        userInfo = match.get("username");
      }
    } else {
      userInfo = null;
    }
    try {
      if (match.containsKey("hdfs-host")) {
        int port = UNSPECIFIED_PORT;
        if (match.containsKey("hdfs-port")) {
          try {
            port = Integer.parseInt(match.get("hdfs-port"));
          } catch (NumberFormatException e) {
            port = UNSPECIFIED_PORT;
          }
        }
        return new URI("hdfs", userInfo, match.get("hdfs-host"),
            port, "/", null, null);
      } else {
        return new URI("file", userInfo, "", UNSPECIFIED_PORT, "/", null, null);
      }
    } catch (URISyntaxException ex) {
      throw new DatasetRepositoryException("Could not build FS URI", ex);
    }
  }

  /**
   * Sets the MetaStore URI in the given Configuration, if there is a host in
   * the match arguments. If there is no host, then the conf is not changed.
   *
   * @param conf a Configuration that will be used to connect to the MetaStore
   * @param match URIPattern match results
   */
  private static void setMetaStoreURI(
      Configuration conf, Map<String, String> match) {
    try {
      int port = UNSPECIFIED_PORT;
      if (match.containsKey("port")) {
        try {
          port = Integer.parseInt(match.get("port"));
        } catch (NumberFormatException e) {
          port = UNSPECIFIED_PORT;
        }
      }
      // if either the host or the port is set, construct a new MetaStore URI
      // and set the property in the Configuration. otherwise, this will not
      // change the connection URI.
      if (match.containsKey("host")) {
         conf.set(HIVE_METASTORE_URI_PROP,
             new URI("thrift", null, match.get("host"), port, "/", null, null)
                 .toString());
      }
    } catch (URISyntaxException ex) {
      throw new DatasetRepositoryException(
          "Could not build metastore URI", ex);
    }
  }
}
