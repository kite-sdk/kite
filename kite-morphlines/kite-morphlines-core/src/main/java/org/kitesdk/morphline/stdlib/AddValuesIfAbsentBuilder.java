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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;

import com.typesafe.config.Config;

/**
 * For each input field value, add the value to the given record output field if the value isn't
 * already contained in that field.
 */
public final class AddValuesIfAbsentBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("addValuesIfAbsent");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new AddValuesIfAbsent(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class AddValuesIfAbsent extends AbstractAddValuesCommand {

    public AddValuesIfAbsent(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected void putAll(Record record, String key, Collection values) {
      List existingValues = record.get(key);
      if (values.size() <= 3) { // fast path for small N
        for (Object value : values) {
          if (!existingValues.contains(value)) {
            existingValues.add(value);
          }
        }
      } else { // index avoids performance degradation for large N
        Set index = new HashSet(existingValues);
        for (Object value : values) {
          if (index.add(value)) {
            existingValues.add(value);
          }
        }
      }
    }
    
    @Override
    protected void put(Record record, String key, Object value) {
      record.putIfAbsent(key, value);
    }
    
  }
    
}
