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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Validator;
import org.kitesdk.morphline.shaded.com.google.code.regexp.GroupInfo;
import org.kitesdk.morphline.shaded.com.google.code.regexp.Matcher;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * The Grok command uses regular expression pattern matching to extract structured fields from
 * unstructured log data.
 * <p>
 * It is perfect for syslog logs, apache and other webserver logs, mysql logs, and in general, any
 * log format that is generally written for humans and not computer consumption.
 */
public final class GrokBuilder implements CommandBuilder {

  /*
   * Uses a shaded version of com.google.code.regexp-0.2.3 to minimize potential dependency issues.
   * See https://github.com/tony19/named-regexp
   */
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("grok");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Grok(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Grok extends AbstractCommand {

    private final List<Regex> regexes = new ArrayList<Regex>();
    private final boolean extract;
    private final boolean extractInPlace;
    private final NumRequiredMatches numRequiredMatches;
    private final boolean findSubstrings;
    private final boolean addEmptyStrings;
    private final String firstKey; // cached value
    private final String renderedConfig; // cached value

    public Grok(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      GrokDictionaries dict = new GrokDictionaries(config, getConfigs());
      Config exprConfig = getConfigs().getConfig(config, "expressions", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(exprConfig)) {
        String expr = entry.getValue().toString();
        this.regexes.add(new Regex(entry.getKey(), dict.compileExpression(expr).matcher("")));
      }
      this.firstKey = (regexes.size() == 0 ? null : regexes.iterator().next().getRecordInputField());

      String extractStr = getConfigs().getString(config, "extract", "true");
      this.extractInPlace = extractStr.equals("inplace");
      if (extractInPlace) {
        this.extract = true;
      } else {
        this.extract = getConfigs().getBoolean(config, "extract", true);
      }
      
      this.numRequiredMatches = new Validator<NumRequiredMatches>().validateEnum(
          config,
          getConfigs().getString(config, "numRequiredMatches", NumRequiredMatches.atLeastOnce.toString()),
          NumRequiredMatches.class);
      this.findSubstrings = getConfigs().getBoolean(config, "findSubstrings", false);
      this.addEmptyStrings = getConfigs().getBoolean(config, "addEmptyStrings", false);
      validateArguments();
      this.renderedConfig = config.root().render();
    }
    
    @Override
    protected boolean doProcess(Record inputRecord) {
      Record outputRecord;
      outputRecord = ((extractInPlace || !extract) ? inputRecord : inputRecord.copy());
      if (extractInPlace) {
        // Ensure that we mutate the record inplace only if *all* expressions match.
        // To ensure this we potentially run doMatch() twice: the first time to check, the second
        // time to mutate
        boolean isFast;
        if (regexes.size() == 0) {
          isFast = true;
        } else if (regexes.size() > 1) {
          isFast = false;
        } else if (numRequiredMatches == NumRequiredMatches.atLeastOnce) {
          isFast = true;
        } else { // all or once
          assert regexes.size() == 1;
          assert firstKey != null;
          isFast = (inputRecord.get(firstKey).size() <= 1);
        }
        
        if (!isFast) {
          if (!doMatch(inputRecord, outputRecord, false)) {
            return false;
          }
        } else {
          ; // no need to do anything
          // This is a performance enhancement for some cases with a single expression:
          // By the time we find a regex match we know that the whole command will succeed,
          // so there's really no need to run doMatch() twice.
        }
      }
      if (!doMatch(inputRecord, outputRecord, extract)) {
        return false;
      }
      
      // pass record to next command in chain:
      return super.doProcess(outputRecord);
    }

    private boolean doMatch(Record inputRecord, Record outputRecord, boolean doExtract) {
      for (Regex regex : regexes) {        
        Matcher matcher = regex.getMatcher();
        List values = inputRecord.get(regex.getRecordInputField());
        int todo = values.size();
        int minMatches = 1;
        int maxMatches = Integer.MAX_VALUE;
        switch (numRequiredMatches) {
          case once : { 
            maxMatches = 1;
            break;
          }
          case all : { 
            minMatches = todo;
            break;
          }
          default: {
            break;
          }
        }        
        int numMatches = 0;
        for (Object value : values) {
          matcher.reset(value.toString());
          if (!findSubstrings) {
            if (matcher.matches()) {
              numMatches++;
              if (numMatches > maxMatches) {
                LOG.debug("grok failed because it found too many matches for values: {} for grok command: {}",
                          values, renderedConfig);
                return false;
              }
              extract(outputRecord, regex, doExtract);
            }
          } else {
            int previousNumMatches = numMatches;
            while (matcher.find()) {
              if (numMatches == previousNumMatches) {
                numMatches++;
                if (numMatches > maxMatches) {
                  LOG.debug("grok failed because it found too many matches for values: {} for grok command: {}",
                            values, renderedConfig);
                  return false;
                }
                if (!doExtract && numMatches >= minMatches && maxMatches == Integer.MAX_VALUE) {
                  break; // fast path
                }
              }
              extract(outputRecord, regex, doExtract);
            }
          }
          todo--;
          if (!doExtract && numMatches >= minMatches && maxMatches == Integer.MAX_VALUE) {
            break; // fast path
          }
        }
        if (numMatches + todo < minMatches) {
          LOG.debug("grok failed because it found too few matches for values: {} for grok command: {}", 
                    values, renderedConfig);
          return false;          
        }
      }
      return true;
    }

    private void extract(Record outputRecord, Regex regex, boolean doExtract) {
      if (doExtract) {
        regex.extract(outputRecord, addEmptyStrings);
      }
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    /* Caches various regex matcher info for best performance */
    private static final class Regex {
      
      private final String recordInputField;
      private final Matcher matcher;
      private final String[] groupNames;
      private final int[] groupNumbers;
          
      public Regex(String recordInputField, Matcher matcher) {
        Preconditions.checkNotNull(recordInputField);
        Preconditions.checkNotNull(matcher);
        this.recordInputField = recordInputField;
        this.matcher = matcher;
        
        int size = 0;
        for (Map.Entry<String, List<GroupInfo>> entry : matcher.namedPattern().groupInfo().entrySet()) {
          size += entry.getValue().size();
        }
        this.groupNames = new String[size]; 
        this.groupNumbers = new int[size];
        
        int i = 0;
        for (Map.Entry<String, List<GroupInfo>> entry : matcher.namedPattern().groupInfo().entrySet()) {
          String groupName = entry.getKey();
          assert groupName != null;
          List<GroupInfo> groupInfos = entry.getValue();
          for (GroupInfo groupInfo : groupInfos) {
            int idx = groupInfo.groupIndex();
            int group = idx > -1 ? idx + 1 : -1;
            groupNames[i] = groupName;
            groupNumbers[i] = group;
            i++;
          }
        }
        assert i == size;
      }
      
      public void extract(Record outputRecord, boolean addEmptyStrings) {
        for (int i = 0; i < groupNumbers.length; i++) {
          String value = matcher.group(groupNumbers[i]);
          if (value != null && (value.length() > 0 || addEmptyStrings)) {
            outputRecord.put(groupNames[i], value);
          }
        }
      }

      public String getRecordInputField() {
        return recordInputField;
      }
      
      public Matcher getMatcher() {
        return matcher;
      }
      
    }     
    
    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static enum NumRequiredMatches {
      atLeastOnce,
      once,
      all     
    }     

  }
  
}
