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
package org.kitesdk.morphline.hadoop.rcfile;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
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
    this.inputStream = new FSDataInputStream(new ForwardOnlySeekable(inputStream));
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

  @Override
  public boolean delete(Path path) throws IOException {
    throw new UnsupportedOperationException("not implemented!");
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Forward-only Seekable InputStream to use for reading SequenceFiles.  Will throw an exception if
   * an attempt is made to seek backwards.
   */
  private static class ForwardOnlySeekable extends InputStream implements Seekable, PositionedReadable {
    private ForwardOnlySeekableInputStream fosInputStream;

    public ForwardOnlySeekable(InputStream inputStream) {
      this.fosInputStream = new ForwardOnlySeekableInputStream(inputStream);
    }

    /**
     * Seek to the given offset from the start of the file.
     * The next read() will be from that location.  Can't
     * seek past the end of the file.
     */
    public void seek(long pos) throws IOException {
      fosInputStream.seek(pos);
    }

    /**
     * Return the current offset from the start of the file
     */
    public long getPos() throws IOException {
      return fosInputStream.tell();
    }

    /**
     * Seeks a different copy of the data.  Returns true if
     * found a new source, false otherwise.
     */
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException("not implemented!");
    }

    /**
     * Read upto the specified number of bytes, from a given
     * position within a file, and return the number of bytes read. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException("not implemented!");
    }

    /**
     * Read the specified number of bytes, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException("not implemented!");
    }

    /**
     * Read number of bytes equal to the length of the buffer, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException("not implemented!");
    }

    public int read() throws IOException {
      byte [] b = new byte[1];
      int len = fosInputStream.read(b, 0, 1);
      int ret = (len == -1)? -1 : b[0] & 0xFF;
      return ret;
    }
  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * A SeekableInput backed by an {@link InputStream} that can only advance
   * forward, not backwards.
   */
  private static final class ForwardOnlySeekableInputStream { // implements SeekableInput {

    private final InputStream in;
    private long pos = 0;

    public ForwardOnlySeekableInputStream(InputStream in) {
      this.in = in;
    }

//    @Override
    public long tell() throws IOException {
      return pos;
    }

//    @Override
    public int read(byte b[], int off, int len) throws IOException {
      int n = in.read(b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

//    @Override
    public long length() throws IOException {
      throw new UnsupportedOperationException("Random access is not supported");
    }

//    @Override
    public void seek(long p) throws IOException {
      long todo = p - pos;
      if (todo < 0) {
        throw new UnsupportedOperationException("Seeking backwards is not supported");
      }
      skip(todo);
    }

    private long skip(long len) throws IOException {
      // borrowed from org.apache.hadoop.io.IOUtils.skipFully()
      len = Math.max(0, len);
      long todo = len;
      while (todo > 0) {
        long ret = in.skip(todo);
        if (ret == 0) {
          // skip may return 0 even if we're not at EOF.  Luckily, we can
          // use the read() method to figure out if we're at the end.
          int b = in.read();
          if (b == -1) {
            throw new EOFException( "Premature EOF from inputStream after " +
                "skipping " + (len - todo) + " byte(s).");
          }
          ret = 1;
        }
        todo -= ret;
        pos += ret;
      }
      return len;
    }

//    @Override
    public void close() throws IOException {
      in.close();
    }

  }
}
