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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
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
import org.kitesdk.morphline.api.MorphlineRuntimeException;
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
    
    private final Map<String, String> mappings = new HashMap();
    private final Schema fixedSchema;
    private final String schemaField;
    
    // more efficient than raising & catching exceptions
    private static final Object ERROR = new Object(); 
    
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
        
        Object avroResult = ERROR;
        if (field.schema().getType() == Schema.Type.ARRAY) {
          avroResult = toAvro(list, field); 
        } else if (list.size() == 0) { 
          try { // this will fail if there is no default value
            avroResult = ReflectData.get().getDefaultValue(field);
          } catch (AvroRuntimeException e) {
            avroResult = ERROR;
          }
        } else if (list.size() == 1) {
          avroResult = toAvro(list.get(0), field); 
        }
        
        if (avroResult == ERROR) {
          LOG.debug("Cannot convert item: {} to schema: {}", list, schema);
          return false;          
        }
        avroRecord.put(field.pos(), avroResult);
      }

      outputRecord.put(Fields.ATTACHMENT_BODY, avroRecord);
        
      // pass record to next command in chain:
      return super.doProcess(outputRecord);
    }
  
    /* returns true if schema allows the value to be null, false otherwise */
    private static boolean nullOk(Schema schema) {
      if (Schema.Type.NULL == schema.getType()) {
        return true;
      } else if (Schema.Type.UNION == schema.getType()) {
        for (Schema candidate : schema.getTypes()) {
          if (nullOk(candidate)) {
            return true;
          }
        }
      }
      return false;
    }
    
    private Object toAvro(Object item, Field field) {
      if (item == null && !nullOk(field.schema())) {
        try { // this will fail if there is no default value
          return ReflectData.get().getDefaultValue(field);
        } catch (AvroRuntimeException e) {
          return ERROR;
        }
      }
      Object result = toAvro(item, field.schema());
      return result;
    }
    
    private Object toAvro(Object item, Schema schema) {
      // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
      // DOUBLE, BOOLEAN, NULL
      switch (schema.getType()) {
        case RECORD:
          if (item instanceof Map) {
            Map<String,Object> map = (Map) item;
            IndexedRecord record = new GenericData.Record(schema);
            for (Field field : schema.getFields()) {
              Object value = map.get(field.name());
              Object result = toAvro(value, field);
              if (result == ERROR) {
                return ERROR;
              }
              record.put(field.pos(), result);
            }
            return record;
          }
          return ERROR;
        case ENUM:
          if (schema.hasEnumSymbol(item.toString())) {
            return item.toString();
          } 
          return ERROR;
        case ARRAY:
          if (item instanceof List) {
            ListIterator iter = ((List)item).listIterator();
            while (iter.hasNext()) {
              Object result = toAvro(iter.next(), schema.getElementType());
              if (result == ERROR) {
                return ERROR;
              }
              iter.set(result);
            }
            return item;
          }
          return ERROR;
        case MAP:
          if (item instanceof Map) {
            Map<String,Object> map = (Map) item;
            for (Map.Entry entry : map.entrySet()) {
              if (!(entry.getKey() instanceof CharSequence)) {
                return ERROR; // Avro requires that map keys are CharSequences 
              }
              Object result = toAvro(entry.getValue(), schema.getValueType());
              if (result == ERROR) {
                return ERROR;
              }
              entry.setValue(result);
            }
            return item;
          }
          return ERROR;
        case UNION:
          return toAvroUnion(item, schema);
        case FIXED:
          if (item instanceof byte[]) {
            return new GenericData.Fixed(schema, (byte[])item);
          }          
          return ERROR;
        case STRING:
          assert item != null;
          return item.toString();
        case BYTES:
          if (item instanceof ByteBuffer) {
            return item;
          }
          if (item instanceof byte[]) {
            return ByteBuffer.wrap((byte[])item);
          }  
          return ERROR;
        case INT:
          if (item instanceof Integer) {
            return item;
          }
          if (item instanceof Number) {
            return ((Number) item).intValue();
          }
          try {
            return Integer.valueOf(item.toString());
          } catch (NumberFormatException e) {
            return ERROR;
          }
        case LONG:
          if (item instanceof Long) {
            return item;
          }
          if (item instanceof Number) {
            return ((Number) item).longValue();
          }
          try {
            return Long.valueOf(item.toString());
          } catch (NumberFormatException e) {
            return ERROR;
          }
        case FLOAT:
          if (item instanceof Float) {
            return item;
          }
          if (item instanceof Number) {
            return ((Number) item).floatValue();
          }
          try {
            return Float.valueOf(item.toString());
          } catch (NumberFormatException e) {
            return ERROR;
          }
        case DOUBLE:
          if (item instanceof Double) {
            return item;
          }
          if (item instanceof Number) {
            return ((Number) item).doubleValue();
          }
          try {
            return Double.valueOf(item.toString());
          } catch (NumberFormatException e) {
            return ERROR;
          }
        case BOOLEAN:
          if (item instanceof Boolean) {
            return item;
          }
          assert item != null;
          String str = item.toString();
          if ("true".equals(str)) {
            return Boolean.TRUE;
          }
          if ("false".equals(str)) {
            return Boolean.FALSE;
          }
          return ERROR;
        case NULL:
          if (item == null) {
            return null;
          }
          return ERROR;
        default:
          throw new MorphlineRuntimeException("Unknown Avro schema type: " + schema.getType());
      }
    }

    private Object toAvroUnion(Object item, Schema schema) {
      assert schema.getType() == Schema.Type.UNION;
      List<Schema> types = schema.getTypes();
      int index = -1;
      if (item instanceof Map) {
        // a map can be converted both into an avro record or an avro map.
        // so there's some ambiguity - we choose which one applies based on specified order.
        for (int j = 0; j < types.size(); j++) {
          Schema.Type t = types.get(j).getType(); 
          if (t == Schema.Type.RECORD || t == Schema.Type.MAP) {
            index = j;
            break;
          }
        }
      } else {
        try {
          // check if there's a perfect fit for a mapping
          index = GenericData.get().resolveUnion(schema, item); // TODO: optimize
        } catch (AvroRuntimeException e) {
          ; // proceed to find first fit based on specified order (see below)
          // LOG.trace("Cannot find perfect fit for item: {} to union schema: {}", item, schema);
        }
      }
      
      if (index >= 0) { // found perfect fit
        Schema candidate = types.get(index);
        Object result = toAvro(item, candidate);
        return result;
      } else { // find first fit based on specified order
        for (Schema candidate : types) {            
          Object result = toAvro(item, candidate);
          if (result != ERROR) {
            return result;
          }
        }
        return ERROR;
      }
    }
    
  }
    
}
