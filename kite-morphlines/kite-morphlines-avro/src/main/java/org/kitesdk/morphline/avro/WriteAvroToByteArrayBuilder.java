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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Command that serializes the Avro records contained in the _attachment_body field into a byte
 * array and replaces the _attachment_body field with that byte array.
 * 
 * @since 0.9.0
 */
public final class WriteAvroToByteArrayBuilder implements CommandBuilder {
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("writeAvroToByteArray");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new WriteAvroToByteArray(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class WriteAvroToByteArray extends AbstractCommand {
    
    private final Format format;
    private final CodecFactory codecFactory;
    private final Map<String,String> metadata = new HashMap();
    
    public WriteAvroToByteArray(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.format = new Validator<Format>().validateEnum(
          config,
          getConfigs().getString(config, "format", Format.container.toString()),
          Format.class);
      
      String codec = getConfigs().getString(config, "codec", null);
      if (codec == null) {
        this.codecFactory = null;
      } else {
        this.codecFactory = CodecFactory.fromString(codec);
      }
      
      Config metadataConfig = getConfigs().getConfig(config, "metadata", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(metadataConfig)) {
        this.metadata.put(entry.getKey(), entry.getValue().toString());
      }
      
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {      
      Record outputRecord = inputRecord.copy();
      AbstractParser.removeAttachments(outputRecord);
      ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
      if (format == Format.container) {
        writeContainer(inputRecord, bout);
      } else {
        writeContainerless(inputRecord, bout);
      }      
      outputRecord.put(Fields.ATTACHMENT_BODY, bout.toByteArray());
        
      // pass record to next command in chain:
      return super.doProcess(outputRecord);
    }

    private void writeContainer(Record src, OutputStream dst) {
      DataFileWriter dataFileWriter = null;
      try {
        try {
          Schema schema = null;
          for (Object attachment : src.get(Fields.ATTACHMENT_BODY)) {
            Preconditions.checkNotNull(attachment);
            GenericContainer datum = (GenericContainer) attachment;
            schema = getSchema(datum, schema);
            if (dataFileWriter == null) { // init
              GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
              dataFileWriter = new DataFileWriter(datumWriter);
              if (codecFactory != null) {
                dataFileWriter.setCodec(codecFactory);
              }
              for (Map.Entry<String,String> entry : metadata.entrySet()) {
                dataFileWriter.setMeta(entry.getKey(), entry.getValue());              
              }
              dataFileWriter.create(schema, dst);
            }
            dataFileWriter.append(datum);
          }
          if (dataFileWriter != null) {
            dataFileWriter.flush();
          }
        } catch (IOException e) {
          throw new MorphlineRuntimeException(e);
        }
      } finally {
        Closeables.closeQuietly(dataFileWriter);
      }
    }
      
    private void writeContainerless(Record src, OutputStream dst) {
      try {
        GenericDatumWriter datumWriter = new GenericDatumWriter();
        Encoder encoder = null;
        Schema schema = null;
        for (Object attachment : src.get(Fields.ATTACHMENT_BODY)) {
          Preconditions.checkNotNull(attachment);
          GenericContainer datum = (GenericContainer) attachment;
          schema = getSchema(datum, schema);
          datumWriter.setSchema(schema);
          if (encoder == null) { // init
            if (format == Format.containerlessJSON) {
              encoder = EncoderFactory.get().jsonEncoder(schema, dst);
            } else {
              encoder = EncoderFactory.get().binaryEncoder(dst, null);
            }          
          } 
          datumWriter.write(datum, encoder);
        }
        encoder.flush();
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      }
    }
    
    private Schema getSchema(GenericContainer datum, Schema lastSchema) {
      Schema schema = datum.getSchema();
      if (lastSchema != null && lastSchema != schema) {
        throw new MorphlineRuntimeException("Schemas must be identical: " + schema + ", lastSchema: " + lastSchema);
      }
      return schema;
    }

  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static enum Format {
    container,
    containerlessJSON,
    containerlessBinary     
  }     

}
