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

import org.kitesdk.compat.DynConstructors;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.hcatalog.HCatalogDatasetRepository;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
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

  private static final Logger LOG = LoggerFactory.getLogger(Loader.class);

  public static final String HIVE_METASTORE_URI_PROP = "hive.metastore.uris";
  private static final int UNSPECIFIED_PORT = -1;
  private static final String ALWAYS_REPLACED = "ALWAYS-REPLACED";
  private static final String HDFS_HOST = "hdfs:host";
  private static final String HDFS_PORT = "hdfs:port";
  private static final String OLD_HDFS_HOST = "hdfs-host";
  private static final String OLD_HDFS_PORT = "hdfs-port";

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
      LOG.debug("External URI options: {}", match);
      final Path root;
      String path = match.get("path");
      if (match.containsKey("absolute")
          && Boolean.valueOf(match.get("absolute"))) {
        root = (path == null || path.isEmpty()) ? new Path("/") : new Path("/", path);
      } else {
        root = (path == null || path.isEmpty()) ? new Path(".") : new Path(path);
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
      LOG.debug("Managed URI options: {}", match);
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
    // Need to use a HiveConf so that the hive-site.xml file is used, but Hive
    // may not be available. If the constructor can't be found, don't register.
    DynConstructors.Ctor<Configuration> hiveConf = new DynConstructors.Builder()
        .impl("org.apache.hadoop.hive.conf.HiveConf")
        .build();

    // get a default Configuration to configure defaults (so it's okay!)
    Configuration conf = hiveConf.newInstance();

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
    // Managed sets use the same URI for both repository and dataset, which
    // means that dataset must be passed as a query argument
    final OptionBuilder<DatasetRepository> managedBuilder =
        new ManagedBuilder(conf);
    URIPattern basic = new URIPattern("hive");
    Registration.register(basic, basic, managedBuilder);
    // add a URI with no path to allow overriding the metastore authority
    // the authority section is *always* a URI without it cannot match and one
    // with a path (so missing authority) also cannot match
    URIPattern basicAuth = new URIPattern("hive://" + ALWAYS_REPLACED);
    Registration.register(basicAuth, basicAuth, managedBuilder);

    // external data sets
    final OptionBuilder<DatasetRepository> externalBuilder =
        new ExternalBuilder(conf);

    String hdfsAuthority;
    try {
      // Use a HDFS URI with no authority and the environment's configuration
      // to find the default HDFS information
      final URI hdfs = FileSystem.get(URI.create("hdfs:/"), conf).getUri();
      hdfsAuthority = "&" + HDFS_HOST + "=" + hdfs.getHost() +
          "&" + HDFS_PORT + "=" + hdfs.getPort();
    } catch (IOException ex) {
      LOG.warn(
          "Could not locate HDFS, hdfs-host and hdfs-port " +
          "will not be set by default for Hive repositories.");
      hdfsAuthority = "";
    }

    Registration.register(
        new URIPattern(
            "hive://" + hiveAuthority + "/*path?absolute=true" + hdfsAuthority
        ),
        new URIPattern(
            "hive://" + hiveAuthority +
                "/*path/:dataset?absolute=true" + hdfsAuthority
        ),
        externalBuilder
    );
    Registration.register(
        new URIPattern("hive:*path"),
        new URIPattern("hive:*path/:dataset"),
        externalBuilder);
  }

  private static URI fileSystemURI(Map<String, String> match) {
    final String userInfo;
    if (match.containsKey(URIPattern.USERNAME)) {
      if (match.containsKey(URIPattern.PASSWORD)) {
        userInfo = match.get(URIPattern.USERNAME) + ":" +
            match.get(URIPattern.PASSWORD);
      } else {
        userInfo = match.get(URIPattern.USERNAME);
      }
    } else {
      userInfo = null;
    }
    try {
      if (match.containsKey(HDFS_HOST) || match.containsKey(OLD_HDFS_HOST)) {
        int port = UNSPECIFIED_PORT;
        if (match.containsKey(HDFS_PORT) || match.containsKey(OLD_HDFS_PORT)) {
          try {
            port = Integer.parseInt(first(match, HDFS_PORT, OLD_HDFS_PORT));
          } catch (NumberFormatException e) {
            port = UNSPECIFIED_PORT;
          }
        }
        return new URI("hdfs", userInfo, first(match, HDFS_HOST, OLD_HDFS_HOST),
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
      if (match.containsKey(URIPattern.HOST)) {
        try {
          port = Integer.parseInt(match.get(URIPattern.PORT));
        } catch (NumberFormatException e) {
          port = UNSPECIFIED_PORT;
        }
      }
      // if either the host or the port is set, construct a new MetaStore URI
      // and set the property in the Configuration. otherwise, this will not
      // change the connection URI.
      if (match.containsKey(URIPattern.HOST)) {
         conf.set(HIVE_METASTORE_URI_PROP,
             new URI("thrift", null, match.get(URIPattern.HOST), port, null,
                 null, null).toString());
      }
    } catch (URISyntaxException ex) {
      throw new DatasetRepositoryException(
          "Could not build metastore URI", ex);
    }
  }

  private static String first(Map<String, String> data, String... keys) {
    for (String key : keys) {
      if (data.containsKey(key)) {
        return data.get(key);
      }
    }
    return null;
  }
}
