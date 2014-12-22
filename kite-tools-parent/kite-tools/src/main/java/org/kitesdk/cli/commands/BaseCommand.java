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

import com.beust.jcommander.internal.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.cli.Command;
import org.kitesdk.data.spi.HadoopFileSystemURLStreamHandler;
import org.slf4j.Logger;

public abstract class BaseCommand implements Command, Configurable {

  @VisibleForTesting
  static final Charset UTF8 = Charset.forName("utf8");

  private static final String RESOURCE_URI_SCHEME = "resource";
  private static final String STDIN_AS_SOURCE = "stdin";

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
    if (STDIN_AS_SOURCE.equals(filename)) {
      return System.in;
    }

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
   * Returns a {@link ClassLoader} for a set of jars and directories.
   *
   * @param jars A list of jar paths
   * @param paths A list of directories containing .class files
   * @throws MalformedURLException
   */
  protected static ClassLoader loaderFor(List<String> jars, List<String> paths)
      throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(jars, paths)));
  }

  /**
   * Returns a {@link ClassLoader} for a set of jars.
   *
   * @param jars A list of jar paths
   * @throws MalformedURLException
   */
  protected static ClassLoader loaderForJars(List<String> jars)
      throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(jars, null)));
  }

  /**
   * Returns a {@link ClassLoader} for a set of directories.
   *
   * @param paths A list of directories containing .class files
   * @throws MalformedURLException
   */
  protected static ClassLoader loaderForPaths(List<String> paths)
      throws MalformedURLException {
    return AccessController.doPrivileged(new GetClassLoader(urls(null, paths)));
  }

  private static List<URL> urls(List<String> jars, List<String> dirs)
      throws MalformedURLException {
    // check the additional jars and lib directories in the local FS
    final List<URL> urls = Lists.newArrayList();
    if (dirs != null) {
      for (String lib : dirs) {
        // final URLs must end in '/' for URLClassLoader
        File path = lib.endsWith("/") ? new File(lib) : new File(lib + "/");
        Preconditions.checkArgument(path.exists(),
            "Lib directory does not exist: " + lib);
        Preconditions.checkArgument(path.isDirectory(),
            "Not a directory: " + lib);
        Preconditions.checkArgument(path.canRead() && path.canExecute(),
            "Insufficient permissions to access lib directory: " + lib);
        urls.add(path.toURI().toURL());
      }
    }
    if (jars != null) {
      for (String jar : jars) {
        File path = new File(jar);
        Preconditions.checkArgument(path.exists(),
            "Jar files does not exist: " + jar);
        Preconditions.checkArgument(path.isFile(),
            "Not a file: " + jar);
        Preconditions.checkArgument(path.canRead(),
            "Cannot read jar file: " + jar);
        urls.add(path.toURI().toURL());
      }
    }
    return urls;
  }

  private static class GetClassLoader implements PrivilegedAction<ClassLoader> {
    private final URL[] urls;

    public GetClassLoader(List<URL> urls) {
      this.urls = urls.toArray(new URL[urls.size()]);
    }

    @Override
    public ClassLoader run() {
      return new URLClassLoader(
          urls, Thread.currentThread().getContextClassLoader());
    }
  }
}
