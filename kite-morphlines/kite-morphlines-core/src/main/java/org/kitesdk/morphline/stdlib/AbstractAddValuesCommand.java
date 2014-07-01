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
package org.kitesdk.morphline.stdlib;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.FieldExpression;

import com.typesafe.config.Config;

/**
 * Base class for addValues/setValues/addValuesIfAbsent commands.
 */
abstract class AbstractAddValuesCommand extends AbstractCommand {
  
  private final Set<Map.Entry<String, Object>> entrySet;
  
  public AbstractAddValuesCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
    super(builder, config, parent, child, context);      
    entrySet = new Configs().getEntrySet(config);
    for (Map.Entry<String, Object> entry : entrySet) {
      if (!(entry.getValue() instanceof Collection)) {
        entry.setValue(new FieldExpression(entry.getValue().toString(), getConfig()));        
      }
    }
  }
      
  @Override
  @SuppressWarnings("unchecked")
  protected boolean doProcess(Record record) { 
    for (Map.Entry<String, Object> entry : entrySet) {
      String fieldName = entry.getKey();
      prepare(record, fieldName);
      Object entryValue = entry.getValue();
      Collection results;
      if (entryValue instanceof Collection) {
        results = (Collection)entryValue;
      } else {
        results = ((FieldExpression) entryValue).evaluate(record);
      }
      putAll(record, fieldName, results);
    }
    
    // pass record to next command in chain:
    return super.doProcess(record);
  }
  
  protected void prepare(Record record, String key) {    
  }
  
  @SuppressWarnings("unchecked")
  protected void putAll(Record record, String key, Collection values) {
    record.getFields().putAll(key, values);
  }
  
  protected void put(Record record, String key, Object value) {
    record.getFields().put(key, value);
  }
  
}
