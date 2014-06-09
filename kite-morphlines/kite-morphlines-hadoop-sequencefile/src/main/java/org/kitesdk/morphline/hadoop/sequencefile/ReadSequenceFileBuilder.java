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
package org.kitesdk.morphline.hadoop.sequencefile;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that emits one record per sequence file entry in the input stream of the first attachment.
 */
public final class ReadSequenceFileBuilder implements CommandBuilder {

  public static final String OUTPUT_MEDIA_TYPE = "application/java-sequence-file-record";
  public static final String SEQUENCE_FILE_META_DATA = "sequenceFileMetaData";
  public static final String CONFIG_KEY_FIELD = "keyField";
  public static final String CONFIG_VALUE_FIELD = "valueField";
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readSequenceFile");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadSequenceFile(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadSequenceFile extends AbstractParser {

    private final boolean includeMetaData;
    private final String keyField;
    private final String valueField;
    private final Configuration conf = new Configuration();
  
    public ReadSequenceFile(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.includeMetaData = getConfigs().getBoolean(config, "includeMetaData", false);
      this.keyField = getConfigs().getString(config, CONFIG_KEY_FIELD, Fields.ATTACHMENT_NAME);
      this.valueField = getConfigs().getString(config, CONFIG_VALUE_FIELD, Fields.ATTACHMENT_BODY);
      validateArguments();
    }
  
    @Override
    protected boolean doProcess(Record inputRecord, final InputStream in) throws IOException {
      SequenceFile.Metadata sequenceFileMetaData = null;
      SequenceFile.Reader reader = null;
      try {
        // Need to pass valid FS and path, although overriding openFile means
        // they will not be used.
        reader = new SequenceFile.Reader(FileSystem.getLocal(conf), new Path("/"), conf) {
          @Override
          protected FSDataInputStream openFile(FileSystem fs, Path f, int sz, long l) throws IOException {
            return new FSDataInputStream(new ForwardOnlySeekable(in));
          }
        };
        if (includeMetaData) {
          sequenceFileMetaData = reader.getMetadata();
        }
        Class keyClass = reader.getKeyClass();
        Class valueClass = reader.getValueClass();
        Record template = inputRecord.copy();
        removeAttachments(template);
        
        while (true) {
          Writable key = (Writable)ReflectionUtils.newInstance(keyClass, conf);
          Writable val = (Writable)ReflectionUtils.newInstance(valueClass, conf);
          try {
            if (!reader.next(key, val)) {
              break;
            }
          } catch (EOFException ex) {
            // SequenceFile.Reader will throw an EOFException after reading
            // all the data, if it doesn't know the length.  Since we are
            // passing in an InputStream, we hit this case;
            LOG.trace("Received expected EOFException", ex);
            break;
          }
          incrementNumRecords();
          Record outputRecord = template.copy();
          outputRecord.put(keyField, key);
          outputRecord.put(valueField, val);
          outputRecord.put(Fields.ATTACHMENT_MIME_TYPE, OUTPUT_MEDIA_TYPE);
          if (includeMetaData && sequenceFileMetaData != null) {
            outputRecord.put(SEQUENCE_FILE_META_DATA, sequenceFileMetaData);
          }
          
          // pass record to next command in chain:
          if (!getChild().process(outputRecord)) {
            return false;
          }
        }
      } finally {
        Closeables.closeQuietly(reader);
      }
      return true;
    }
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
   * A SeekableInput backed by an {@link java.io.InputStream} that can only advance
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
