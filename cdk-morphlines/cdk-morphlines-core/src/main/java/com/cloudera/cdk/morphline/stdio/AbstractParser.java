/**
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
package com.cloudera.cdk.morphline.stdio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Metrics;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Base class for convenient implementation of morphline parsers.
 */
public abstract class AbstractParser extends AbstractCommand {

  protected final Counter numRecordsCounter;
  private Set<MediaType> supportedMimeTypes = null;

  public static final String SUPPORTED_MIME_TYPES = "supportedMimeTypes";
  
  public AbstractParser(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);      
    List<String> mimeTypes = getConfigs().getStringList(config, SUPPORTED_MIME_TYPES, null);
    if (mimeTypes != null) {
      for (String streamMediaType : mimeTypes) {
        addSupportedMimeType(streamMediaType);
      }
    }
    this.numRecordsCounter = getCounter(Metrics.NUM_RECORDS);
  }

  protected void addSupportedMimeType(String mediaType) {
    if (supportedMimeTypes == null) {
      supportedMimeTypes = new HashSet();
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

