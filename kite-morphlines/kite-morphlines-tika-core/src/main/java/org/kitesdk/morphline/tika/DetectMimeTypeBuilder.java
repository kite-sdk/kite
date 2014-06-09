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
package org.kitesdk.morphline.tika;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.tika.config.ServiceLoader;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Preconditions;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that auto-detects the MIME type of the first attachment, if no MIME type is defined yet.
 */
public final class DetectMimeTypeBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("detectMimeType");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new DetectMimeType(this, config, parent, child, context);
    } catch (IOException e) {
      throw new MorphlineCompilationException("Cannot instantiate command", config, e, this);
    } catch (MimeTypeException e) {
      throw new MorphlineCompilationException("Cannot instantiate command", config, e, this);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class DetectMimeType extends AbstractCommand {

    private final Detector detector;
    private final boolean preserveExisting;
    private final boolean includeMetaData;
    private final boolean excludeParameters;
    
    public DetectMimeType(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) throws IOException, MimeTypeException {
      super(builder, config, parent, child, context);
      this.preserveExisting = getConfigs().getBoolean(config, "preserveExisting", true);      
      this.includeMetaData = getConfigs().getBoolean(config, "includeMetaData", false);
      this.excludeParameters = getConfigs().getBoolean(config, "excludeParameters", true);
      List<InputStream> inputStreams = new ArrayList();
      try {
        if (getConfigs().getBoolean(config, "includeDefaultMimeTypes", true)) {
          // adapted from Tika MimeTypesFactory.create(String coreFilePath, String extensionFilePath)
          String coreFilePath = "tika-mimetypes.xml"; 
          String classPrefix = MimeTypesFactory.class.getPackage().getName().replace('.', '/') + "/"; 
          ClassLoader cl = MimeTypesFactory.class.getClassLoader();       
          URL coreURL = cl.getResource(classPrefix + coreFilePath);
          InputStream in = new BufferedInputStream(coreURL.openStream());
          inputStreams.add(in);
        }
        for (String mimeTypesFile : getConfigs().getStringList(config, "mimeTypesFiles", Collections.<String>emptyList())) {
          InputStream in = new BufferedInputStream(new FileInputStream(new File(mimeTypesFile)));
          inputStreams.add(in);
        }
        String mimeTypesString = getConfigs().getString(config, "mimeTypesString", null);
        if (mimeTypesString != null) {
          InputStream in = new ByteArrayInputStream(mimeTypesString.getBytes("UTF-8"));
          inputStreams.add(in);
        }
        
        if (inputStreams.size() > 0) {
          MimeTypes mimeTypes = MimeTypesFactory.create(inputStreams.toArray(new InputStream[inputStreams.size()]));
          ServiceLoader loader = new ServiceLoader();
          this.detector = new DefaultDetector(mimeTypes, loader);
        } else {
          throw new MorphlineCompilationException("Missing specification for MIME type mappings", config);
        }      
      } finally {
        for (InputStream in : inputStreams) {
          Closeables.closeQuietly(in);
        }
      }
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record record) {
      if (preserveExisting && record.getFields().containsKey(Fields.ATTACHMENT_MIME_TYPE)) {
        ; // we must preserve the existing MIME type
      } else {
        List attachments = record.get(Fields.ATTACHMENT_BODY);
        if (attachments.size() > 0) {
          Object attachment = attachments.get(0);
          Preconditions.checkNotNull(attachment);
          InputStream stream;
          if (attachment instanceof byte[]) {
            stream = new ByteArrayInputStream((byte[]) attachment);
          } else {
            stream = (InputStream) attachment;
          }
          
          Metadata metadata = new Metadata();
          
          // If you specify the resource name (the filename, roughly) with this
          // parameter, then Tika can use it in guessing the right MIME type
          String resourceName = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
          if (resourceName != null) {
            metadata.add(Metadata.RESOURCE_NAME_KEY, resourceName);
          }

          // Provide stream's charset as hint to Tika for better auto detection
          String charset = (String) record.getFirstValue(Fields.ATTACHMENT_CHARSET);
          if (charset != null) {
            metadata.add(Metadata.CONTENT_ENCODING, charset);
          }

          if (includeMetaData) {
            for (Entry<String, Object> entry : record.getFields().entries()) {
              metadata.add(entry.getKey(), entry.getValue().toString());
            }
          }

          String mimeType = getMediaType(stream, metadata, excludeParameters);
          record.replaceValues(Fields.ATTACHMENT_MIME_TYPE, mimeType);
        }  
      }
      return super.doProcess(record);
    }
    
    /**
     * Detects the content type of the given input event. Returns
     * <code>application/octet-stream</code> if the type of the event can not be
     * detected.
     * <p>
     * It is legal for the event headers or body to be empty. The detector may
     * read bytes from the start of the body stream to help in type detection.
     * 
     * @return detected media type, or <code>application/octet-stream</code>
     */
    private String getMediaType(InputStream in, Metadata metadata, boolean excludeParameters) {
      MediaType mediaType;
      try {
        mediaType = getDetector().detect(in, metadata);
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      }
      String mediaTypeStr = mediaType.toString();
      if (excludeParameters) {
        int i = mediaTypeStr.indexOf(';');
        if (i >= 0) {
          mediaTypeStr = mediaTypeStr.substring(0, i);
        }
      }
      return mediaTypeStr;
    }

    protected Detector getDetector() {
      return detector;
    }

  }
  
}
