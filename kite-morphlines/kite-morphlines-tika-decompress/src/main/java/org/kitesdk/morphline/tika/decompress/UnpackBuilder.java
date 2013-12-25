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
package org.kitesdk.morphline.tika.decompress;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.mime.MediaType;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that unpacks the first attachment. Implementation adapted from Tika PackageParser.
 */
public final class UnpackBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("unpack");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Unpack(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Unpack extends AbstractParser {
    
    private static final String GTAR = "application/x-gtar";

    private static final MediaType ZIP = MediaType.APPLICATION_ZIP;
    private static final MediaType JAR = MediaType.application("java-archive");
    private static final MediaType AR = MediaType.application("x-archive");
    private static final MediaType CPIO = MediaType.application("x-cpio");
    private static final MediaType DUMP = MediaType.application("x-tika-unix-dump");
    private static final MediaType TAR = MediaType.application("x-tar");

    private static final Set<MediaType> SUPPORTED_TYPES =
            MediaType.set(ZIP, JAR, AR, CPIO, DUMP, TAR);
    
    public Unpack(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      if (!config.hasPath(SUPPORTED_MIME_TYPES)) {
//        for (MediaType mediaType : new PackageParser().getSupportedTypes(new ParseContext())) {
        for (MediaType mediaType : SUPPORTED_TYPES) {
          addSupportedMimeType(mediaType.toString());
        }
        addSupportedMimeType(GTAR); // apparently not already included in PackageParser.getSupportedTypes()
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record, InputStream stream) {
      EmbeddedExtractor extractor = new EmbeddedExtractor();

      // At the end we want to close the compression stream to release
      // any associated resources, but the underlying document stream
      // should not be closed
      stream = new CloseShieldInputStream(stream);

      // Ensure that the stream supports the mark feature
      stream = new BufferedInputStream(stream);

      ArchiveInputStream ais;
      try {
        ArchiveStreamFactory factory = new ArchiveStreamFactory();
        ais = factory.createArchiveInputStream(stream);
      } catch (ArchiveException e) {
        throw new MorphlineRuntimeException("Unable to unpack document stream", e);
      }

      try {
        ArchiveEntry entry = ais.getNextEntry();
        while (entry != null) {
          if (!entry.isDirectory()) {
            if (!parseEntry(ais, entry, extractor, record)) {
              return false;
            }
          }
          entry = ais.getNextEntry();
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        Closeables.closeQuietly(ais);
      }
      return true;
    }

    private boolean parseEntry(ArchiveInputStream archive, ArchiveEntry entry, EmbeddedExtractor extractor, Record record) {
      String name = entry.getName();
      if (archive.canReadEntryData(entry)) {
        Record entrydata = new Record(); // TODO: or pass myself?
        //Record entrydata = record.copy();
        
        // For detectors to work, we need a mark/reset supporting
        // InputStream, which ArchiveInputStream isn't, so wrap
        TemporaryResources tmp = new TemporaryResources();
        try {
          TikaInputStream tis = TikaInputStream.get(archive, tmp);
          return extractor.parseEmbedded(tis, entrydata, name, getChild());
        } finally {
          try {
            tmp.dispose();
          } catch (TikaException e) {
            LOG.warn("Cannot dispose of tmp Tika resources", e);
          }
        }
      } else {
        return false;
      } 
    }
    
  }

}
