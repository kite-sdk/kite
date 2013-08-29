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

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * Command that iterates over the items in a given record input field, interprets each item as a key-value
 * pair where the key and value are separated by the given separator character, and adds the pair's
 * value to the record field named after the pair's key.
 */
public final class SplitKeyValueBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("splitKeyValue");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new SplitKeyValue(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SplitKeyValue extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldPrefix;
    private final char separatorChar;
    private final boolean addEmptyStrings;
    private final boolean trim;
    
    public SplitKeyValue(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");      
      this.outputFieldPrefix = getConfigs().getString(config, "outputFieldPrefix", "");
      String separator = getConfigs().getString(config, "separator", "=");
      if (separator.length() != 1) {
        throw new MorphlineCompilationException("separator must be one character only: " + separator, config);
      }
      this.separatorChar = separator.charAt(0);
      this.addEmptyStrings = getConfigs().getBoolean(config, "addEmptyStrings", false);      
      this.trim = getConfigs().getBoolean(config, "trim", true);
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Object item : record.get(inputFieldName)) {
        String str = item.toString();
        String key = str;
        String value = "";
        int i = str.indexOf(separatorChar);
        if (i >= 0) {
          key = str.substring(0, i);
          value = str.substring(i + 1, str.length());
        }
        value = trim(value);        
        if (value.length() > 0 || addEmptyStrings) {
          record.put(outputFieldPrefix + trim(key), value);
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

    private String trim(String str) {
      return trim ? str.trim() : str;
    }
    
  }
  
}
