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
package com.cloudera.cdk.morphline.hadoop.rcfile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Very simple Filesystem Implementation which serves an InputStream for a given
 * path. This is used to serve the underlying input stream from a FileSystem
 * Interface. Only open() and getFileStatus() is implemented.
 */
public final class SingleStreamFileSystem extends FileSystem {
  private final FSDataInputStream inputStream;
  private final Path path;
  private final FileStatus fileStatus;

  public SingleStreamFileSystem(InputStream inputStream, Path path)
      throws IOException {
    this.inputStream = new FSDataInputStream(inputStream);
    this.path = path;
    // Since this is a stream, we dont know the length of the stream. Setting it
    // to the maximum size
    this.fileStatus = new FileStatus(Long.MAX_VALUE, false, 0, 0, 0, path);
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize)
      throws IOException {
    if (f.equals(path)) {
      return inputStream;
    }
    throw new UnsupportedOperationException("Path " + f.getName()
        + " is not found");
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public boolean delete(final Path f, final boolean recursive)
      throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws FileNotFoundException,
      IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public void setWorkingDirectory(final Path new_dir) {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission)
      throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    if (path.equals(f)) {
      return fileStatus;
    }
    throw new UnsupportedOperationException("Path " + f.getName()
        + " is not found");
  }
}
