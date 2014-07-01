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
package org.kitesdk.morphline.avro;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


/**
 * Command that parses an InputStream that contains Avro binary container file data; for each Avro
 * datum, the command emits a morphline record containing the datum as an attachment in
 * {@link Fields#ATTACHMENT_BODY}.
 * 
 * The Avro schema that was used to write the Avro data is retrieved from the container. Optionally, the
 * Avro schema that shall be used for reading can be supplied as well.
 */
public final class ReadAvroContainerBuilder implements CommandBuilder {

  /** The MIME type that input attachments shall have */
  public static final String MIME_TYPE = "avro/binary";
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readAvroContainer");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadAvroContainer(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  static class ReadAvroContainer extends AbstractParser {

    protected final Schema readerSchema;
    protected FastGenericDatumReader<GenericContainer> datumReader;
    private final Map<ByteArrayKey, ResolvingDecoder> resolverCache;

    public ReadAvroContainer(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {   
      super(builder, config, parent, child, context);

      String schemaString = getConfigs().getString(config, "readerSchemaString", null);
      if (schemaString != null) {
        this.readerSchema = new Parser().parse(schemaString);
      } else {        
        String schemaFile = getConfigs().getString(config, "readerSchemaFile", null);
        if (schemaFile != null) {
          try { 
            this.readerSchema = new Parser().parse(new File(schemaFile));
          } catch (IOException e) {
            throw new MorphlineCompilationException("Cannot parse external Avro reader schema file: " + schemaFile, config, e);
          }
        } else {
          this.readerSchema = null;
        }
      }
      
      if (getClass() == ReadAvroContainer.class) {
        resolverCache = new BoundedLRUHashMap<ByteArrayKey, ResolvingDecoder>(
            getConfigs().getInt(config, "schemaCacheCapacity", 100));
        
        validateArguments();
      } else {
        resolverCache = null;
      }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {
      if (datumReader == null) { // reuse for performance
        datumReader = new FastGenericDatumReader(null, readerSchema);
      }
      DataFileReader<GenericContainer> reader = null;
      try {
        // TODO: for better performance subclass DataFileReader 
        // to eliminate expensive SchemaParser.parse() on each new file in DataFileReader.initialize(). 
        // Instead replace the parse() with a lookup in the byte[] cache map.
        reader = new DataFileReader(new ForwardOnlySeekableInputStream(in), datumReader);
        
        byte[] writerSchemaBytes = reader.getMeta(DataFileConstants.SCHEMA);
        Preconditions.checkNotNull(writerSchemaBytes);
        ByteArrayKey writerSchemaKey = new ByteArrayKey(writerSchemaBytes);
        ResolvingDecoder resolver = resolverCache.get(writerSchemaKey); // cache for performance
        if (resolver == null) { 
          resolver = createResolver(datumReader.getSchema(), datumReader.getExpected());
          resolverCache.put(writerSchemaKey, resolver);
          datumReader.setResolver(resolver);
        }
        Record template = inputRecord.copy();
        removeAttachments(template);
        template.put(Fields.ATTACHMENT_MIME_TYPE, ReadAvroBuilder.AVRO_MEMORY_MIME_TYPE);
        while (reader.hasNext()) {
          GenericContainer datum = reader.next();
          if (!extract(datum, template)) {
            return false;
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
      return true;
    }
    
    protected ResolvingDecoder createResolver(Schema writerSchema, Schema readerSchema) throws IOException {
      return DecoderFactory.get().resolvingDecoder(
          Schema.applyAliases(writerSchema, readerSchema), readerSchema, null);
    }
    
    protected boolean extract(GenericContainer datum, Record inputRecord) {
      incrementNumRecords();
      Record outputRecord = inputRecord.copy();
      outputRecord.put(Fields.ATTACHMENT_BODY, datum);
        
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class BoundedLRUHashMap<K,V> extends LinkedHashMap<K,V> {
    
    private final int capacity;

    private BoundedLRUHashMap(int capacity) {
      super(16, 0.5f, true);
      this.capacity = capacity;
    }
    
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest) {
      return size() > capacity;
    }
      
  } 

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ByteArrayKey {
    
    private byte[] bytes;
    
    public ByteArrayKey(byte[] bytes) {
      this.bytes = bytes;
    }
    
    @Override
    public boolean equals(Object other) {
      ByteArrayKey otherKey = (ByteArrayKey) other;
      return Arrays.equals(bytes, otherKey.bytes);
    }
    
    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * A {@link SeekableInput} backed by an {@link InputStream} that can only advance
   * forward, not backwards.
   */
  static final class ForwardOnlySeekableInputStream implements SeekableInput {
    // class is public for testing only!
    
    private final InputStream in;
    private long pos = 0;
    
    public ForwardOnlySeekableInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public long tell() throws IOException {
      return pos;
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      int n = in.read(b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }
    
    @Override
    public long length() throws IOException {
      throw new UnsupportedOperationException("Random access is not supported");
    }

    @Override
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
    
    @Override
    public void close() throws IOException {
      in.close();
    }
    
  }
}
