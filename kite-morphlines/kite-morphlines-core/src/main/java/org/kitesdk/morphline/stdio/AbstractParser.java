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
package org.kitesdk.morphline.stdio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;

import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;

import com.typesafe.config.Config;

/**
 * Base class for convenient implementation of morphline parsers.
 */
public abstract class AbstractParser extends AbstractCommand {

  private final Meter numRecordsMeter;
  private Set<MediaType> supportedMimeTypes = null;

  public static final String SUPPORTED_MIME_TYPES = "supportedMimeTypes";

  protected AbstractParser(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    super(builder, config, parent, child, context);      
    List<String> mimeTypes = getConfigs().getStringList(config, SUPPORTED_MIME_TYPES, Collections.<String>emptyList());
    for (String mimeType : mimeTypes) {
      addSupportedMimeType(mimeType);
    }
    this.numRecordsMeter = getMeter(Metrics.NUM_RECORDS);
  }

  /** Deprecated; will be removed in the next release */
  @Deprecated
  protected AbstractParser(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);      
    List<String> mimeTypes = getConfigs().getStringList(config, SUPPORTED_MIME_TYPES, Collections.<String>emptyList());
    for (String mimeType : mimeTypes) {
      addSupportedMimeType(mimeType);
    }
    this.numRecordsMeter = getMeter(Metrics.NUM_RECORDS);
  }

  protected void addSupportedMimeType(String mediaType) {
    if (supportedMimeTypes == null) {
      supportedMimeTypes = Sets.newHashSet();
    }
    supportedMimeTypes.add(parseMimeType(mediaType));
  }

  @Override
  protected boolean doProcess(Record record) {
    if (!hasAtLeastOneAttachment(record)) {
      return false;
    }

    // TODO: make field for stream configurable
    String streamMediaType = (String) record.getFirstValue(Fields.ATTACHMENT_MIME_TYPE);
    if (!isMimeTypeSupported(streamMediaType, record)) {
      return false;
    }

    InputStream stream = getAttachmentInputStream(record);
    try {
      return doProcess(record, stream);
    } catch (IOException e) {
      throw new MorphlineRuntimeException(e);
    } finally {
      Closeables.closeQuietly(stream);
    }
  }
  
  protected abstract boolean doProcess(Record record, InputStream stream) throws IOException;

  protected void incrementNumRecords() {
    if (isMeasuringMetrics()) {
      numRecordsMeter.mark();
    }
  }
  
  private boolean isMimeTypeSupported(String mediaTypeStr, Record record) {
    if (supportedMimeTypes == null) {
      return true;
    }
    if (!hasAtLeastOneMimeType(record)) {
      return false;
    }
    MediaType mediaType = parseMimeType(mediaTypeStr);
    if (supportedMimeTypes.contains(mediaType)) {
      return true; // fast path
    }
    // wildcard matching
    for (MediaType rangePattern : supportedMimeTypes) {      
      if (isMimeTypeMatch(mediaType, rangePattern)) {
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("No supported MIME type found for " + Fields.ATTACHMENT_MIME_TYPE + "=" + mediaTypeStr);
    }
    return false;
  }

  private MediaType parseMimeType(String mediaTypeStr) {
    MediaType mediaType = MediaType.parse(mediaTypeStr.trim().toLowerCase(Locale.ROOT));
    return mediaType.getBaseType();
  };
      
  /** Returns true if mediaType falls withing the given range (pattern), false otherwise */
  private boolean isMimeTypeMatch(MediaType mediaType, MediaType rangePattern) {
    String WILDCARD = "*";
    String rangePatternType = rangePattern.getType();
    String rangePatternSubtype = rangePattern.getSubtype();
    return (rangePatternType.equals(WILDCARD) || rangePatternType.equals(mediaType.getType()))
        && (rangePatternSubtype.equals(WILDCARD) || rangePatternSubtype.equals(mediaType.getSubtype()));
  }

  protected Charset detectCharset(Record record, Charset charset) {
    if (charset != null) {
      return charset;
    }
    List charsets = record.get(Fields.ATTACHMENT_CHARSET);
    if (charsets.size() == 0) {
      // TODO try autodetection (AutoDetectReader)
      throw new MorphlineRuntimeException("Missing charset for record: " + record); 
    }
    String charsetName = (String) charsets.get(0);        
    return Charset.forName(charsetName);
  }

  private boolean hasAtLeastOneAttachment(Record record) {
    if (!record.getFields().containsKey(Fields.ATTACHMENT_BODY)) {
      LOG.debug("Command failed because of missing attachment for record: {}", record);
      return false;
    }
    return true;
  }
  
  private boolean hasAtLeastOneMimeType(Record record) {
    if (!record.getFields().containsKey(Fields.ATTACHMENT_MIME_TYPE)) {
      LOG.debug("Command failed because of missing MIME type for record: {}", record);
      return false;
    }  
    return true;
  }

  private InputStream getAttachmentInputStream(Record record) {
    Object body = record.getFirstValue(Fields.ATTACHMENT_BODY);
    Preconditions.checkNotNull(body);
    if (body instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) body);
    } else {
      return (InputStream) body;
    }
  }

  public static void removeAttachments(Record outputRecord) {
    outputRecord.removeAll(Fields.ATTACHMENT_BODY);
    outputRecord.removeAll(Fields.ATTACHMENT_MIME_TYPE);
    outputRecord.removeAll(Fields.ATTACHMENT_CHARSET);
    outputRecord.removeAll(Fields.ATTACHMENT_NAME);
  }
  
  int getBufferSize(InputStream stream) {
    if (stream instanceof ByteArrayInputStream) {
      return 1024; // probably a single log line from Flume    
    } else {
      return 8192; // same as default for new BufferedReader()
    }
  }


//public static XMediaType toGuavaMediaType(TMediaType tika) {
//return XMediaType.create(tika.getType(), tika.getSubtype()).withParameters(Multimaps.forMap(tika.getParameters()));
//}
//
//public static List<XMediaType> toGuavaMediaType(Iterable<TMediaType> tikaCollection) {
//List<XMediaType> list = new ArrayList();
//for (TMediaType tika : tikaCollection) {
//  list.add(toGuavaMediaType(tika));
//}
//return list;
//}

}

