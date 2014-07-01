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
package org.kitesdk.morphline.json;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;


/**
 * Command that uses zero or more JSON path expressions to extract values from a JSON object.
 * 
 * The JSON input object is expected to be contained in the {@link Fields#ATTACHMENT_BODY}
 * 
 * Each expression consists of a record output field name (on the left side of the colon ':') as
 * well as zero or more path steps (on the right hand side), each path step separated by a '/'
 * slash. JSON arrays are traversed with the '[]' notation.
 * 
 * The result of a path expression is a list of objects, each of which is added to the given record
 * output field.
 * 
 * The path language supports all JSON concepts, including nested structures, records, arrays, etc,
 * as well as a flatten option that collects the primitives in a subtree into a flat list.
 */
public final class ExtractJsonPathsBuilder implements CommandBuilder {
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractJsonPaths");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractJsonPaths(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ExtractJsonPaths extends AbstractCommand {
    
    private final boolean flatten;
    private final Map<String, Collection<String>> stepMap;
    
    private static final String ARRAY_TOKEN = "[]";

    public ExtractJsonPaths(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
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
      JsonNode datum = (JsonNode) inputRecord.getFirstValue(Fields.ATTACHMENT_BODY);
      Preconditions.checkNotNull(datum);
      Record outputRecord = inputRecord.copy();
      
      for (Map.Entry<String, Collection<String>> entry : stepMap.entrySet()) {
        String fieldName = entry.getKey();
        List<String> steps = (List<String>) entry.getValue();
        extractPath(datum, fieldName, steps, outputRecord, 0);
      }
        
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }

    private void extractPath(JsonNode datum, String fieldName, List<String> steps, Record record, int level) {
      if (level >= steps.size()) {
        return;
      }
      boolean isLeaf = (level + 1 == steps.size());
      String step = steps.get(level);
      if (ARRAY_TOKEN == step) {
        if (datum.isArray()) {
          if (isLeaf) {
            resolve(datum, record, fieldName);
          } else {
            Iterator<JsonNode> iter = datum.elements();
            while (iter.hasNext()) {
              extractPath(iter.next(), fieldName, steps, record, level + 1);
            }
          }
        }
      } else if (datum.isObject()) {
        JsonNode value = datum.get(step); 
        if (value != null) {
          if (isLeaf) {
            resolve(value, record, fieldName);
          } else {
            extractPath(value, fieldName, steps, record, level + 1);
          }
        }
      } 
    }
    
    private void resolve(JsonNode datum, Record record, String fieldName) { 
      if (datum == null) {
        return;
      }
      
      if (flatten) {
        flatten(datum, record.get(fieldName));
        return;
      }

      if (datum.isObject()) {
        record.put(fieldName, datum);
      } else if (datum.isArray()) {
        record.put(fieldName, datum);  
      } else if (datum.isTextual()) {
        record.put(fieldName, datum.asText());
      } else if (datum.isBoolean()) {
        record.put(fieldName, datum.asBoolean());
      } else if (datum.isInt()) {
        record.put(fieldName, datum.asInt());
      } else if (datum.isLong()) {
        record.put(fieldName, datum.asLong());
      } else if (datum.isShort()) {
        record.put(fieldName, datum.shortValue());
      } else if (datum.isDouble()) {
        record.put(fieldName, datum.asDouble());
      } else if (datum.isFloat()) {
        record.put(fieldName, datum.floatValue());
      } else if (datum.isBigInteger()) {
        record.put(fieldName, datum.bigIntegerValue());
      } else if (datum.isBigDecimal()) {
        record.put(fieldName, datum.decimalValue());
      } else if (datum.isNull()) {
        ; // ignore
      } else {
        record.put(fieldName, datum.toString());
      }
    }

    @SuppressWarnings("unchecked")
    private void flatten(JsonNode datum, List list) { 
      if (datum == null) {
        return;
      }

      if (datum.isObject()) {
        for (JsonNode child : datum) {
          flatten(child, list);
        }
      } else if (datum.isArray()) {
        Iterator<JsonNode> iter = datum.elements();
        while (iter.hasNext()) {
          flatten(iter.next(), list);
        }        
      } else if (datum.isTextual()) {
        list.add(datum.asText());
      } else if (datum.isBoolean()) {
        list.add(datum.asBoolean());
      } else if (datum.isInt()) {
        list.add(datum.asInt());
      } else if (datum.isLong()) {
        list.add(datum.asLong());
      } else if (datum.isShort()) {
        list.add(datum.shortValue());
      } else if (datum.isDouble()) {
        list.add(datum.asDouble());
      } else if (datum.isFloat()) {
        list.add(datum.floatValue());
      } else if (datum.isBigInteger()) {
        list.add(datum.bigIntegerValue());
      } else if (datum.isBigDecimal()) {
        list.add(datum.decimalValue());
      } else if (datum.isNull()) {
        ; // ignore
      } else {
        list.add(datum.toString());
      }
    }
    
  }
  
}
