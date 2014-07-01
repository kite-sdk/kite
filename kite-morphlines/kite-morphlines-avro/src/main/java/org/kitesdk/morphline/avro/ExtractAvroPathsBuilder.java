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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;


/**
 * Command that uses zero or more avro path expressions to extract values from an Avro object.
 * 
 * The Avro input object is expected to be contained in the {@link Fields#ATTACHMENT_BODY}
 * 
 * Each expression consists of a record output field name (on the left side of the colon ':') as
 * well as zero or more path steps (on the right hand side), each path step separated by a '/'
 * slash. Avro arrays are traversed with the '[]' notation.
 * 
 * The result of a path expression is a list of objects, each of which is added to the given record
 * output field.
 * 
 * The path language supports all Avro concepts, including nested structures, records, arrays, maps,
 * unions, etc, as well as a flatten option that collects the primitives in a subtree into a flat
 * list.
 */
public final class ExtractAvroPathsBuilder implements CommandBuilder {
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractAvroPaths");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractAvroPaths(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ExtractAvroPaths extends AbstractCommand {
    
    private final boolean flatten;
    private final Map<String, Collection<String>> stepMap;
    
    private static final String ARRAY_TOKEN = "[]";

    public ExtractAvroPaths(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      ListMultimap<String, String> stepMultiMap = ArrayListMultimap.create();
      this.flatten = getConfigs().getBoolean(config, "flatten", true);
      Config paths = getConfigs().getConfig(config, "paths");
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(paths)) {
        String fieldName = entry.getKey();        
        String path = entry.getValue().toString().trim();
        if (path.contains("//")) {
          throw new MorphlineCompilationException("No support for descendant axis available yet", config);
        }
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        if (path.endsWith("/")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        for (String step : path.split("/")) {
          step = step.trim();
          if (step.length() > ARRAY_TOKEN.length() && step.endsWith(ARRAY_TOKEN)) {
            step = step.substring(0,  step.length() - ARRAY_TOKEN.length());
            stepMultiMap.put(fieldName, normalize(step));
            stepMultiMap.put(fieldName, ARRAY_TOKEN);
          } else {
            stepMultiMap.put(fieldName, normalize(step));
          }
        }
      }
      this.stepMap = stepMultiMap.asMap();
      LOG.debug("stepMap: {}", stepMap);
      validateArguments();
    }
    
    private String normalize(String step) { // for faster subsequent query performance
      return ARRAY_TOKEN.equals(step) ? ARRAY_TOKEN : step;
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {
//      Preconditions.checkState(ReadAvroBuilder.AVRO_MEMORY_MIME_TYPE.equals(inputRecord.getFirstValue(Fields.ATTACHMENT_MIME_TYPE)));
      GenericContainer datum = (GenericContainer) inputRecord.getFirstValue(Fields.ATTACHMENT_BODY);
      Preconditions.checkNotNull(datum);
      Preconditions.checkNotNull(datum.getSchema());      
      Record outputRecord = inputRecord.copy();
      
      for (Map.Entry<String, Collection<String>> entry : stepMap.entrySet()) {
        String fieldName = entry.getKey();
        List<String> steps = (List<String>) entry.getValue();
        extractPath(datum, datum.getSchema(), fieldName, steps, outputRecord, 0);
      }
        
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }

    @SuppressWarnings("unchecked")
    private void extractPath(Object datum, Schema schema, String fieldName, List<String> steps, Record record, int level) {
      if (level >= steps.size()) {
        return;
      }
      boolean isLeaf = (level + 1 == steps.size());
      String step = steps.get(level);
      if (ARRAY_TOKEN == step) {
        if (schema.getType() == Type.ARRAY) {
          if (isLeaf) {
            resolve(datum, schema, record, fieldName);
          } else {
            Iterator iter = ((Collection) datum).iterator();
            while (iter.hasNext()) {
              extractPath(iter.next(), schema.getElementType(), fieldName, steps, record, level + 1);
            }
          }
        }
      } else {
        if (schema.getType() == Type.RECORD) {
          GenericRecord genericAvroRecord = (GenericRecord) datum;
          Object value = genericAvroRecord.get(step);
          if (value != null) {
            Schema childSchema = schema.getField(step).schema();
            if (isLeaf) {
              resolve(value, childSchema, record, fieldName);
            } else {
              extractPath(value, childSchema, fieldName, steps, record, level + 1);
            }
          }
        } else if (schema.getType() == Type.MAP) {
          Map<CharSequence, ?> map = (Map<CharSequence, ?>) datum;
          Object value = map.get(step);
          if (value == null) {
            value = map.get(new Utf8(step)); // TODO: fix performance - maybe fix polymorphic weirdness in upstream avro?
          }
          if (value != null) {
            Schema childSchema = schema.getValueType();
            if (isLeaf) {
              resolve(value, childSchema, record, fieldName);
            } else {
              extractPath(value, childSchema, fieldName, steps, record, level + 1);
            }
          }            
        } else if (schema.getType() == Type.UNION) {
          int index = GenericData.get().resolveUnion(schema, datum);
          //String typeName = schema.getTypes().get(index).getName();
          extractPath(datum, schema.getTypes().get(index), fieldName, steps, record, level);
        }
      } 
    }
    
    private void resolve(Object datum, Schema schema, Record record, String fieldName) { 
      if (datum == null) {
        return;
      }
      
      if (flatten) {
        flatten(datum, schema, record.get(fieldName));
        return;
      }

      // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
      // DOUBLE, BOOLEAN, NULL
      switch (schema.getType()) {
      case RECORD: {
        record.put(fieldName, datum);
        break;
      }
      case ENUM: {
        GenericEnumSymbol symbol = (GenericEnumSymbol) datum;
        record.put(fieldName, symbol.toString());
        break;
      }
      case ARRAY: {        
        record.put(fieldName, datum);
        break;
      }
      case MAP: {
        record.put(fieldName, datum);
        break;
      }
      case UNION: {
        record.put(fieldName, normalizeUtf8(datum));
        break;
      }
      case FIXED: {
        GenericFixed fixed = (GenericFixed) datum;
        record.put(fieldName, fixed.bytes());
        break;
      }
      case BYTES: {
        ByteBuffer buf = (ByteBuffer) datum;
        int pos = buf.position();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        buf.position(pos); // undo relative read
        record.put(fieldName, bytes);
        break;
      }
      case STRING: {
        record.put(fieldName, datum.toString());
        break;
      }
      case INT: {
        record.put(fieldName, datum);
        break;
      }
      case LONG: {
        record.put(fieldName, datum);
        break;
      }
      case FLOAT: {
        record.put(fieldName, datum);
        break;
      }
      case DOUBLE: {
        record.put(fieldName, datum);
        break;
      }
      case BOOLEAN: {
        record.put(fieldName, datum);
        break;
      }
      case NULL: {
        break;
      }
      default:
        throw new MorphlineRuntimeException("Unknown Avro schema type: " + schema.getType());
      }
    }
    
    private Object normalizeUtf8(Object datum) {
      if (datum instanceof Utf8) {
        return ((Utf8) datum).toString();
      } else {
        return datum;
      }
    }

    @SuppressWarnings("unchecked")
    private void flatten(Object datum, Schema schema, List list) { 
      if (datum == null) {
        return;
      }

      // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
      // DOUBLE, BOOLEAN, NULL
      switch (schema.getType()) {
      case RECORD: {
        IndexedRecord avroRecord = (IndexedRecord) datum;
        for (Field field : schema.getFields()) {
          flatten(avroRecord.get(field.pos()), field.schema(), list);
        }
        break;
      }
      case ENUM: {
        GenericEnumSymbol symbol = (GenericEnumSymbol) datum;
        list.add(symbol.toString());
        break;
      }
      case ARRAY: {        
        Iterator iter = ((Collection) datum).iterator();
        while (iter.hasNext()) {
          flatten(iter.next(), schema.getElementType(), list);
        }
        break;
      }
      case MAP: {
        Map<CharSequence, ?> map = (Map<CharSequence, ?>) datum;
        for (Map.Entry<CharSequence, ?> entry : map.entrySet()) {
          flatten(entry.getValue(), schema.getValueType(), list);
        }
        break;
      }
      case UNION: {
        int index = GenericData.get().resolveUnion(schema, datum);
        flatten(datum, schema.getTypes().get(index), list);
        break;
      }
      case FIXED: {
        GenericFixed fixed = (GenericFixed) datum;
        list.add(fixed.bytes());
        break;
      }
      case BYTES: {
        ByteBuffer buf = (ByteBuffer) datum;
        int pos = buf.position();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        buf.position(pos); // undo relative read
        list.add(bytes);
        break;
      }
      case STRING: {
        list.add(datum.toString());
        break;
      }
      case INT: {
        list.add(datum);
        break;
      }
      case LONG: {
        list.add(datum);
        break;
      }
      case FLOAT: {
        list.add(datum);
        break;
      }
      case DOUBLE: {
        list.add(datum);
        break;
      }
      case BOOLEAN: {
        list.add(datum);
        break;
      }
      case NULL: {
        break;
      }
      default:
        throw new MorphlineRuntimeException("Unknown Avro schema type: " + schema.getType());
      }
    }
    
  }
  
}
