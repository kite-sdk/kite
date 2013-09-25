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
package com.cloudera.cdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.typesafe.config.Config;

/**
 * Command that examines each string value in a given field and replaces it with the replacement
 * value defined in a given dictionary aka hash table.
 */
public final class TranslateBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("translate");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Translate(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Translate extends AbstractCommand {

    private final String fieldName;
    private final Map<String, Object> dictionary = new HashMap();
    private final Object fallback;
    
    public Translate(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      this.fieldName = getConfigs().getString(config, "field");
      Config dict = getConfigs().getConfig(config, "dictionary");
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(dict)) {
        dictionary.put(entry.getKey().toString(), entry.getValue());
      }
      this.fallback = getConfigs().getString(config, "fallback", null);
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      ListIterator iter = record.get(fieldName).listIterator();
      while (iter.hasNext()) {
        String key = iter.next().toString();
        Object value = dictionary.get(key);
        if (value != null) {
          iter.set(value);
        } else if (fallback != null) {
          iter.set(fallback);
        } else {
          return false;
        }
      }

      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
  }
 
}
