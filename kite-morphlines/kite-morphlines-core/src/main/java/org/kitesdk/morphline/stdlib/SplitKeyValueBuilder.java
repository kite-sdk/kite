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
import java.util.regex.Matcher;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

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
    return new SplitKeyValue(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SplitKeyValue extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldPrefix;
    private final String separator;
    private final Matcher regex;
    private final boolean addEmptyStrings;
    private final boolean trim;
    
    public SplitKeyValue(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");      
      this.outputFieldPrefix = getConfigs().getString(config, "outputFieldPrefix", "");
      this.separator = getConfigs().getString(config, "separator", "=");
      if (separator.length() == 0) {
        throw new MorphlineCompilationException("separator must not be the empty string", config);
      }
      if (getConfigs().getBoolean(config, "isRegex", false)) {
        GrokDictionaries dict = new GrokDictionaries(config, getConfigs());
        this.regex = dict.compileExpression(separator).pattern().matcher("");
      } else {
        this.regex = null;
      }
      this.addEmptyStrings = getConfigs().getBoolean(config, "addEmptyStrings", false);      
      this.trim = getConfigs().getBoolean(config, "trim", true);
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Object item : record.get(inputFieldName)) {
        String str = item.toString();
        int start;
        int end;
        if (regex != null) {
          if (regex.reset(str).find()) {
            start = regex.start();
            end = regex.end();
          } else {
            start = -1;
            end = -1;
          }   
        } else if (separator.length() == 1) {
          start = str.indexOf(separator.charAt(0));
          end = start + 1;
        } else {
          start = str.indexOf(separator);
          end = start + separator.length();
        }
        
        String key = str;
        String value = "";        
        if (start >= 0) { // found?
          key = str.substring(0, start);
          value = str.substring(end, str.length());
          value = trim(value);        
        }
        if (value.length() > 0 || addEmptyStrings) {
          record.put(concat(outputFieldPrefix, trim(key)), value);
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

    private String trim(String str) {
      return trim ? str.trim() : str;
    }
    
    private String concat(String x, String y) {
      return x.length() == 0 ? y : x + y;
    }
    
  }
  
}
