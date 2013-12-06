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

package org.kitesdk.data.filesystem.impl;

import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
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

/**
 * A Loader implementation to register URIs for FileSystemDatasetRepositories.
 */
public class Loader implements Loadable {

  private static final Logger logger = LoggerFactory.getLogger(Loader.class);
  private static final int UNSPECIFIED_PORT = -1;

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
        fs = FileSystem.get(fileSystemURI(match), envConf);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(
            "Could not get a FileSystem", ex);
      }
      return (DatasetRepository) new FileSystemDatasetRepository.Builder()
          .configuration(new Configuration(envConf)) // make a modifiable copy
          .rootDirectory(fs.makeQualified(root))
          .build();
    }
  }

  @Override
  public void load() {
    // get a default Configuration to configure defaults (so it's okay!)
    final Configuration conf = new Configuration();
    final OptionBuilder<DatasetRepository> builder =
        new URIBuilder(conf);

    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("file:/*path?absolute=true")), builder);
    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("file:*path")), builder);

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

    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create(
            "hdfs://" + hdfsAuthority + "/*path?absolute=true")),
        builder);
    org.kitesdk.data.impl.Accessor.getDefault().registerDatasetRepository(
        new URIPattern(URI.create("hdfs:*path")), builder);
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
      int port = UNSPECIFIED_PORT;
      if (match.containsKey("port")) {
        try {
          port = Integer.parseInt(match.get("port"));
        } catch (NumberFormatException e) {
          port = UNSPECIFIED_PORT;
        }
      }
      return new URI(match.get("scheme"), userInfo, match.get("host"), port, "/",
          null, null);
    } catch (URISyntaxException ex) {
      throw new DatasetRepositoryException("Could not build FS URI", ex);
    }
  }
}
