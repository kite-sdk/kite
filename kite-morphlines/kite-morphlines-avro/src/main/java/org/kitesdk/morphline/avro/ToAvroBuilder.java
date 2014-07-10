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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Command that converts a morphline record to an Avro record.
 *
 * @since 0.9.0
 */
public final class ToAvroBuilder implements CommandBuilder {
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toAvro");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToAvro(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ToAvro extends AbstractCommand {
    
    private final Map<String, String> mappings = new HashMap<String,String>();
    private final Schema fixedSchema;
    private final String schemaField;
    
    public ToAvro(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      String schemaFile = getConfigs().getString(config, "schemaFile", null);
      String schemaString = getConfigs().getString(config, "schemaString", null);
      this.schemaField = getConfigs().getString(config, "schemaField", null);
      
      int numDefinitions = 0;
      if (schemaFile != null) {
        numDefinitions++;
      }
      if (schemaString != null) {
        numDefinitions++;
      }
      if (schemaField != null) {
        numDefinitions++;
      }
      if (numDefinitions == 0) {
        throw new MorphlineCompilationException(
          "Either schemaFile or schemaString or schemaField must be defined", config);
      }
      if (numDefinitions > 1) {
        throw new MorphlineCompilationException(
          "Must define only one of schemaFile or schemaString or schemaField at the same time", config);
      }

      if (schemaString != null) {
        this.fixedSchema = new Parser().parse(schemaString);
      } else if (schemaFile != null) {
        try { 
          this.fixedSchema = new Parser().parse(new File(schemaFile));
        } catch (IOException e) {
          throw new MorphlineCompilationException(
            "Cannot parse external Avro schema file: " + schemaFile, config, e);
        }
      } else {
        this.fixedSchema = null;
      }
      
      Config mappingsConfig = getConfigs().getConfig(config, "mappings", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(mappingsConfig)) {
        mappings.put(entry.getKey(), entry.getValue().toString());
      }
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {
      Schema schema;
      if (schemaField != null) {
        schema = (Schema) inputRecord.getFirstValue(schemaField);
        Preconditions.checkNotNull(schema);
      } else {
        schema = fixedSchema;
      }
      
      Record outputRecord = inputRecord.copy();
      AbstractParser.removeAttachments(outputRecord);
      IndexedRecord avroRecord = new GenericData.Record(schema);
      
      for (Field field : schema.getFields()) {
        String morphlineFieldName = mappings.get(field.name());
        if (morphlineFieldName == null) {
          morphlineFieldName = field.name();
        }
        List list = inputRecord.get(morphlineFieldName);
        
        Object avroResult = AvroConversions.ERROR;
        if (field.schema().getType() == Schema.Type.ARRAY) {
          avroResult = AvroConversions.toAvro(list, field);
        } else if (list.size() == 0) { 
          try { // this will fail if there is no default value
            avroResult = ReflectData.get().getDefaultValue(field);
          } catch (AvroRuntimeException e) {
            avroResult = AvroConversions.ERROR;
          }
        } else if (list.size() == 1) {
          avroResult = AvroConversions.toAvro(list.get(0), field); 
        }
        
        if (avroResult == AvroConversions.ERROR) {
          LOG.debug("Cannot convert item: {} to schema: {}", list, schema);
          return false;          
        }
        avroRecord.put(field.pos(), avroResult);
      }

      outputRecord.put(Fields.ATTACHMENT_BODY, avroRecord);
        
      // pass record to next command in chain:
      return super.doProcess(outputRecord);
    }  
    
  }
    
}
