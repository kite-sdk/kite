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
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;

/**
 * Command that divides strings into substrings, by recognizing a <i>separator</i> (a.k.a.
 * "delimiter") which can be expressed as a single character, literal string, regular expression,
 * {@link CharMatcher}, or by using a fixed substring length. This class provides the functionality
 * of Guava's {@link Splitter} class as a morphline command.
 * 
 * This command also supports grok dictionaries in the same way as the {@link GrokBuilder}.
 */
public final class SplitBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("split");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Split(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Split extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldName;
    private final Splitter splitter;
    
    public Split(Config config, Command parent, Command child, MorphlineContext context) {
      // TODO: also add separate command for withKeyValueSeparator?
      super(config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");
      this.outputFieldName = getConfigs().getString(config, "outputField");
      
      String separator = getConfigs().getString(config, "separator");
      boolean isRegex = getConfigs().getBoolean(config, "isRegex", false);
      GrokDictionaries dict = new GrokDictionaries(config, getConfigs());
      Splitter currentSplitter;
      if (isRegex) {
        currentSplitter = Splitter.on(dict.compileExpression(separator).pattern());
      } else if (separator.length() == 1) {
        currentSplitter = Splitter.on(separator.charAt(0));
      } else {
        currentSplitter = Splitter.on(separator);
      }
      
      int limit = getConfigs().getInt(config, "limit", -1);
      if (limit > 0) {
        currentSplitter = currentSplitter.limit(limit);
      }
      
      if (getConfigs().getBoolean(config, "addEmptyStrings", false)) {
        currentSplitter = currentSplitter.omitEmptyStrings();
      }
      
      if (getConfigs().getBoolean(config, "trim", true)) {
        currentSplitter = currentSplitter.trimResults();
      }
      
      this.splitter = currentSplitter;
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Object value : record.get(inputFieldName)) {      
        record.getFields().putAll(outputFieldName, splitter.split(value.toString()));
      }
      return super.doProcess(record);
    }
    
  }
  
}
