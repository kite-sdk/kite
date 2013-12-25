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
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.mime.MediaType;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that decompresses the first attachment. Implementation adapted from Tika CompressorParser.
 */
public final class DecompressBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("decompress");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Decompress(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Decompress extends AbstractParser {
    
    private boolean decompressConcatenated = false; // TODO remove as obsolete
    
    private static final MediaType BZIP = MediaType.application("x-bzip");
    private static final MediaType BZIP2 = MediaType.application("x-bzip2");
    private static final MediaType GZIP = MediaType.application("x-gzip");
    private static final MediaType XZ = MediaType.application("x-xz");
    private static final MediaType PACK = MediaType.application("application/x-java-pack200");

    private static final Set<MediaType> SUPPORTED_TYPES =
            MediaType.set(BZIP, BZIP2, GZIP, XZ, PACK);

    public Decompress(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      if (!config.hasPath(SUPPORTED_MIME_TYPES)) {
//        for (MediaType mediaType : new CompressorParser().getSupportedTypes(new ParseContext())) {
        for (MediaType mediaType : SUPPORTED_TYPES) {
          addSupportedMimeType(mediaType.toString());
        }
      }
      validateArguments();
    }
 
    @Override
    protected boolean doProcess(Record record, InputStream stream) {
      EmbeddedExtractor extractor = new EmbeddedExtractor();

      String name = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
      if (name != null) {
        if (name.endsWith(".tbz")) {
          name = name.substring(0, name.length() - 4) + ".tar";
        } else if (name.endsWith(".tbz2")) {
          name = name.substring(0, name.length() - 5) + ".tar";
        } else if (name.endsWith(".bz")) {
          name = name.substring(0, name.length() - 3);
        } else if (name.endsWith(".bz2")) {
          name = name.substring(0, name.length() - 4);
        } else if (name.endsWith(".xz")) {
          name = name.substring(0, name.length() - 3);
        } else if (name.endsWith(".pack")) {
          name = name.substring(0, name.length() - 5);
        } else if (name.length() > 0) {
          name = GzipUtils.getUncompressedFilename(name);
        }
      }

      // At the end we want to close the compression stream to release
      // any associated resources, but the underlying document stream
      // should not be closed
      stream = new CloseShieldInputStream(stream);

      // Ensure that the stream supports the mark feature
      stream = new BufferedInputStream(stream);

      CompressorInputStream cis;
      try {
        CompressorStreamFactory factory = new CompressorStreamFactory();
        cis = factory.createCompressorInputStream(stream);
      } catch (CompressorException e) {
        throw new MorphlineRuntimeException("Unable to uncompress document stream", e);
      }

      try {
        return extractor.parseEmbedded(cis, record, name, getChild());
      } finally {
        Closeables.closeQuietly(cis);
      }
    }
  }

}
