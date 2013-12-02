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

package com.cloudera.cdk.data.hcatalog.impl;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.hcatalog.HCatalogDatasetRepository;
import com.cloudera.cdk.data.spi.Loadable;
import com.cloudera.cdk.data.spi.OptionBuilder;
import com.cloudera.cdk.data.spi.URIPattern;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Loader implementation to register URIs for FileSystemDatasetRepositories.
 */
public class Loader implements Loadable {

  private static final Logger logger = LoggerFactory.getLogger(Loader.class);

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
      logger.info("Match options: {}", match);
      final Path root;
      String path = match.get("path");
      if (path == null || path.isEmpty()) {
        root = new Path(".");
      } else if (match.containsKey("absolute")
          && Boolean.valueOf(match.get("absolute"))) {
        root = new Path("/", path);
      } else {
        root = new Path(path);
      }
      final FileSystem fs;
      try {
        fs = FileSystem.get(fileSystemURI("hdfs", match), envConf);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(
            "Could not get a FileSystem", ex);
      }
      return (DatasetRepository) new HCatalogDatasetRepository.Builder()
          .configuration(new Configuration(envConf)) // make a modifiable copy
          .rootDirectory(fs.makeQualified(root))
          .build();
    }

    private URI fileSystemURI(String scheme, Map<String, String> match) {
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
        int port = -1;
        if (match.containsKey("hdfs-port")) {
          try {
            port = Integer.parseInt(match.get("hdfs-port"));
          } catch (NumberFormatException e) {
            port = -1;
          }
        }
        return new URI(scheme, userInfo, match.get("hdfs-host"),
            port, "/", null, null);
      } catch (URISyntaxException ex) {
        throw new DatasetRepositoryException("Could not build FS URI", ex);
      }
    }
  }

  private static class ManagedBuilder implements OptionBuilder<DatasetRepository> {
    private final Configuration envConf;

    public ManagedBuilder(Configuration envConf) {
      this.envConf = envConf;
    }

    @Override
    public DatasetRepository getFromOptions(Map options) {
      return new HCatalogDatasetRepository.Builder()
          .configuration(new Configuration(envConf)) // make a modifiable copy
          .build();
    }
  }

  @Override
  public void load() {
    // get a default Configuration to configure defaults (so it's okay!)
    final Configuration conf = new Configuration();

    // Hive-managed data sets
    DatasetRepositories.register(
        new URIPattern(URI.create("hive")), new ManagedBuilder(conf));

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

    // rather than using the URI authority to pass the HDFS host and port, the
    // URI is registered to pass these options as query arguments. This is
    // the most flexible URI pattern for now, while it is not clear if the
    // authority section should be used to specify the connection info for the
    // Hive MetaStore.

    DatasetRepositories.register(
        new URIPattern(URI.create("hive:/*path?absolute=true" + hdfsAuthority)),
        externalBuilder);
    DatasetRepositories.register(
        new URIPattern(URI.create("hive:*path")), externalBuilder);
  }
}
