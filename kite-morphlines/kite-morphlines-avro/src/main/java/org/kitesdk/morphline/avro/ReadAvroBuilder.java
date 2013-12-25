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
import java.util.Collection;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.avro.ReadAvroContainerBuilder.ReadAvroContainer;
import org.kitesdk.morphline.base.Fields;

import com.typesafe.config.Config;


/**
 * Command that parses an InputStream that contains Avro data; for each Avro datum, the command
 * emits a morphline record containing the datum as an attachment in {@link Fields#ATTACHMENT_BODY}.
 * 
 * The Avro schema that was used to write the Avro data must be explicitly supplied. Optionally, the
 * Avro schema that shall be used for reading can be supplied as well.
 */
public final class ReadAvroBuilder implements CommandBuilder {

  /** The MIME type identifier that will be filled into output records */
  public static final String AVRO_MEMORY_MIME_TYPE = "avro/java+memory";

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("readAvro");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadAvro(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  final static class ReadAvro extends ReadAvroContainer {

    private final Schema writerSchema;
    private final boolean isJson;
    private BinaryDecoder binaryDecoder = null;
    private JsonDecoder jsonDecoder = null;
    
    public ReadAvro(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      String schemaString = getConfigs().getString(config, "writerSchemaString", null);
      if (schemaString != null) {
        this.writerSchema = new Parser().parse(schemaString);
      } else {        
        String schemaFile = getConfigs().getString(config, "writerSchemaFile", null);
        if (schemaFile != null) {
          try { 
            this.writerSchema = new Parser().parse(new File(schemaFile));
          } catch (IOException e) {
            throw new MorphlineCompilationException("Cannot parse external Avro writer schema file: " + schemaFile, config, e);
          }
        } else {
          this.writerSchema = null;
        }
      }
      
      this.isJson = getConfigs().getBoolean(config, "isJson", false);
      validateArguments();      
    }
    
    @Override
    protected void validateArguments() {
      super.validateArguments();
      if (writerSchema == null) {
        throw new MorphlineCompilationException(
            "You must specify an external Avro writer schema because this is required to read containerless Avro", getConfig());
      }
    }
    
    @Override
    protected boolean doProcess(Record inputRecord, InputStream in) throws IOException {
      Record template = inputRecord.copy();
      removeAttachments(template);
      template.put(Fields.ATTACHMENT_MIME_TYPE, ReadAvroBuilder.AVRO_MEMORY_MIME_TYPE);
      Decoder decoder = prepare(in);
      try {
        while (true) {
          GenericContainer datum = datumReader.read(null, decoder);
          if (!extract(datum, template)) {
            return false;
          }
        }
      } catch (EOFException e) { 
        ; // ignore
      } finally {
        in.close();
      }
      return true;
    }

    private Decoder prepare(InputStream in) throws IOException {
      Decoder decoder;
      if (isJson) {
        if (jsonDecoder == null) {
          jsonDecoder = DecoderFactory.get().jsonDecoder(writerSchema, in);
        } else {
          jsonDecoder.configure(in); // reuse for performance
        }
        decoder = jsonDecoder;
      } else {        
        binaryDecoder = DecoderFactory.get().binaryDecoder(in, binaryDecoder); // reuse for performance
        decoder = binaryDecoder;
      }
            
      if (datumReader == null) { // reuse for performance
        Schema readSchema = readerSchema != null ? readerSchema : writerSchema;
        datumReader = new FastGenericDatumReader<GenericContainer>(writerSchema, readSchema);  
        datumReader.setResolver(createResolver(writerSchema, readSchema));
      }
      return decoder;
    }
    
  }  
   
}
