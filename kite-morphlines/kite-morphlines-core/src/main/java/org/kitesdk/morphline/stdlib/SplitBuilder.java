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
import java.util.Iterator;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

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
    return new Split(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Split extends AbstractCommand {

    private final String inputFieldName;
    private final String outputFieldName;
    private final List<String> outputFieldNames;
    private final boolean addEmptyStrings;
    private final Splitter splitter;
    
    public Split(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.inputFieldName = getConfigs().getString(config, "inputField");
      
      this.outputFieldName = getConfigs().getString(config, "outputField", null);
      this.outputFieldNames = getConfigs().getStringList(config, "outputFields", null);
      if (outputFieldName == null && outputFieldNames == null) {
        throw new MorphlineCompilationException("Either outputField or outputFields must be defined", config);
      }
      if (outputFieldName != null && outputFieldNames != null) {
        throw new MorphlineCompilationException("Must not define both outputField and outputFields at the same time", config);
      }
      
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
      
      this.addEmptyStrings = getConfigs().getBoolean(config, "addEmptyStrings", false);      
      if (outputFieldNames == null && !addEmptyStrings) {
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
        Iterable<String> columns = splitter.split(value.toString());
        if (outputFieldNames == null) {
          record.getFields().putAll(outputFieldName, columns);
        } else {
          extractColumns(record, columns);
        }
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

    private void extractColumns(Record record, Iterable<String> columns) {
      Iterator<String> iter = columns.iterator();
      for (int i = 0; i < outputFieldNames.size() && iter.hasNext(); i++) {
        String columnValue = iter.next();
        String columnName = outputFieldNames.get(i);
        if (columnName.length() > 0) { // empty column name indicates omit this field on output
          if (columnValue.length() > 0 || addEmptyStrings) {
            record.put(columnName, columnValue);
          }
        }
      }
    }
    
  }
  
}
