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

package com.cloudera.cdk.data.filesystem.impl;

import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetRepositoryException;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.cloudera.cdk.data.spi.Loadable;
import com.cloudera.cdk.data.spi.OptionBuilder;
import com.cloudera.cdk.data.spi.URIPattern;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.commons.configuration.MapConfiguration;
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
  private static class URIBuilder implements OptionBuilder<DatasetRepository> {

    private final Configuration envConf;

    public URIBuilder(Configuration envConf) {
      this.envConf = envConf;
    }

    @Override
    public DatasetRepository getFromOptions(Map<String, String> match) {
      final MapConfiguration options = new MapConfiguration(match);
      final Path root;
      String path = options.getString("path");
      if (path == null || path.isEmpty()) {
        root = new Path("/");
      } else if (options.getBoolean("absolute", false)) {
        root = new Path("/", path);
      } else {
        root = new Path(path);
      }
      final FileSystem fs;
      try {
        fs = FileSystem.get(fileSystemURI(options), envConf);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(
            "Could not get a FileSystem", ex);
      }
      return (DatasetRepository) new FileSystemDatasetRepository.Builder()
          .rootDirectory(root)
          .fileSystem(fs)
          .get();
    }

    private URI fileSystemURI(MapConfiguration options) {
      final String userInfo;
      if (options.containsKey("username")) {
        if (options.containsKey("password")) {
          userInfo = options.getString("password") + ":" +
              options.getString("username");
        } else {
          userInfo = options.getString("username");
        }
      } else {
        userInfo = null;
      }
      try {
        return new URI(options.getString("scheme"), userInfo,
            options.getString("host"), options.getInteger("port", -1),
            "/", null, null);
      } catch (URISyntaxException ex) {
        throw new DatasetRepositoryException("Could not build FS URI", ex);
      }
    }
  }

  @Override
  public void load() {
    // get a default Configuration to configure defaults (so it's okay!)
    final Configuration conf = new Configuration();
    final OptionBuilder<DatasetRepository> builder =
        new URIBuilder(conf);

    DatasetRepositories.register(
        new URIPattern(URI.create("file:*path")), builder);
    DatasetRepositories.register(
        new URIPattern(URI.create("file:/*path?absolute=true")), builder);

    String hdfsAuthority;
    try {
      // Use a HDFS URI with no authority and the environment's configuration
      // to find the default HDFS information
      final URI hdfs = FileSystem.get(URI.create("hdfs:/"), conf).getUri();
      hdfsAuthority = hdfs.getAuthority();
    } catch (IOException ex) {
      logger.warn(
          "Could not locate HDFS, host and port will not be set by default.");
      hdfsAuthority = "";
    }

    DatasetRepositories.register(
        new URIPattern(URI.create(
            "hdfs://" + hdfsAuthority + "/*path?absolute=true")),
        builder);
  }
}
