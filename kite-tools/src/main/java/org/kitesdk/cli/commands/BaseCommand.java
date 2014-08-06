/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.cli.commands;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.cli.Command;
import org.kitesdk.compat.DynMethods;
import org.kitesdk.data.spi.HadoopFileSystemURLStreamHandler;
import org.slf4j.Logger;

public abstract class BaseCommand implements Command, Configurable {

  private static final DynMethods.UnboundMethod addJarURL =
      new DynMethods.Builder("addURL")
          .hiddenImpl(URLClassLoader.class, URL.class)
          .build();

  @VisibleForTesting
  static final Charset UTF8 = Charset.forName("utf8");

  private static final String RESOURCE_URI_SCHEME = "resource";

  private Configuration conf = null;
  private LocalFileSystem localFS = null;

  /**
   * @return FileSystem to use when no file system scheme is present in a path
   * @throws IOException
   */
  public FileSystem defaultFS() throws IOException {
    if (localFS == null) {
      this.localFS = FileSystem.getLocal(getConf());
    }
    return localFS;
  }

  /**
   * Output content to the console or a file.
   *
   * This will not produce checksum files.
   *
   * @param content String content to write
   * @param console A {@link Logger} for writing to the console
   * @param filename The destination {@link Path} as a String
   * @throws IOException
   */
  public void output(String content, Logger console, String filename)
      throws IOException {
    if (filename == null || "-".equals(filename)) {
      console.info(content);
    } else {
      FSDataOutputStream outgoing = create(filename);
      try {
        outgoing.write(content.getBytes(UTF8));
      } finally {
        outgoing.close();
      }
    }
  }

  /**
   * Creates a file and returns an open {@link FSDataOutputStream}.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * This will not produce checksum files and will overwrite a file that
   * already exists.
   *
   * @param filename The filename to create
   * @return An open FSDataOutputStream
   * @throws IOException
   */
  public FSDataOutputStream create(String filename) throws IOException {
    return create(filename, true);
  }

  /**
   * Creates a file and returns an open {@link FSDataOutputStream}.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * This will produce checksum files and will overwrite a file that already
   * exists.
   *
   * @param filename The filename to create
   * @return An open FSDataOutputStream
   * @throws IOException
   */
  public FSDataOutputStream createWithChecksum(String filename)
      throws IOException {
    return create(filename, false);
  }

  private FSDataOutputStream create(String filename, boolean noChecksum)
      throws IOException {
    Path filePath = qualifiedPath(filename);
    // even though it was qualified using the default FS, it may not be in it
    FileSystem fs = filePath.getFileSystem(getConf());
    if (noChecksum && fs instanceof ChecksumFileSystem) {
      fs = ((ChecksumFileSystem) fs).getRawFileSystem();
    }
    return fs.create(filePath, true /* overwrite */);
  }

  /**
   * Returns a qualified {@link Path} for the {@code filename}.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to qualify
   * @return A qualified Path for the filename
   * @throws IOException
   */
  public Path qualifiedPath(String filename) throws IOException {
    Path cwd = defaultFS().makeQualified(new Path("."));
    return new Path(filename).makeQualified(defaultFS().getUri(), cwd);
  }

  /**
   * Returns a {@link URI} for the {@code filename} that is a qualified Path or
   * a resource URI.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to qualify
   * @return A qualified URI for the filename
   * @throws IOException
   */
  public URI qualifiedURI(String filename) throws IOException {
    URI fileURI = URI.create(filename);
    if (RESOURCE_URI_SCHEME.equals(fileURI.getScheme())) {
      return fileURI;
    } else {
      return qualifiedPath(filename).toUri();
    }
  }

  /**
   * Opens an existing file or resource.
   *
   * If the file does not have a file system scheme, this uses the default FS.
   *
   * @param filename The filename to open.
   * @return An open InputStream with the file contents
   * @throws IOException
   * @throws IllegalArgumentException If the file does not exist
   */
  public InputStream open(String filename) throws IOException {
    URI uri = qualifiedURI(filename);
    if (RESOURCE_URI_SCHEME.equals(uri.getScheme())) {
      return Resources.getResource(uri.getRawSchemeSpecificPart()).openStream();
    } else {
      Path filePath = new Path(uri);
      // even though it was qualified using the default FS, it may not be in it
      FileSystem fs = filePath.getFileSystem(getConf());
      return fs.open(filePath);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    HadoopFileSystemURLStreamHandler.setDefaultConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Adds a list of jar paths to the current ClassLoader and the distributed
   * cache.
   * @param jars
   * @throws IOException
   */
  protected void addJars(List<String> jars) throws IOException {
    if (jars != null && !jars.isEmpty()) {
      DynMethods.BoundMethod addJar = addJarURL.bind(
          Thread.currentThread().getContextClassLoader());
      for (String jar : jars) {
        File jarFile = new File(jar);
        DistCache.addJarToDistributedCache(getConf(), jarFile);
        // add to the current loader
        addJar.invoke(jarFile.toURI().toURL());
      }
    }
  }
}
