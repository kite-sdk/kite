/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kitesdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

final class ReplaceValues extends AbstractCommand {
  
  private final PatternNameMatcher nameMatcher;
  private final PatternNameMatcher valueMatcher;
  private final String replacement;

  public ReplaceValues(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context,
      boolean isRemoving) {
    super(builder, config, parent, child, context);
    List<String> nameBlacklist = getConfigs().getStringList(config, "nameBlacklist", Collections.singletonList("*"));
    List<String> nameWhitelist = getConfigs().getStringList(config, "nameWhitelist", Collections.<String>emptyList());
    List<String> valueBlacklist = getConfigs().getStringList(config, "valueBlacklist", Collections.singletonList("*"));
    List<String> valueWhitelist = getConfigs().getStringList(config, "valueWhitelist", Collections.<String>emptyList());
    int nameCacheCapacity = getConfigs().getInt(config, "nameCacheCapacity", 10000);
    this.nameMatcher = new PatternNameMatcher(nameBlacklist, nameWhitelist, nameCacheCapacity);
    int valueCacheCapacity = getConfigs().getInt(config, "valueCacheCapacity", 0);
    this.valueMatcher = new PatternNameMatcher(valueBlacklist, valueWhitelist, valueCacheCapacity);
    this.replacement = isRemoving ? null : getConfigs().getString(config, "replacement");
    validateArguments();
  }

  @Override
  protected boolean doProcess(Record record) {
    if (nameMatcher.getLiteralsOnly() == null) {
      doProcessSlow(record); // general case
    } else { 
      doProcessFast(record); // fast path for common special case
    }
    
    // pass record to next command in chain:
    return super.doProcess(record);
  }

  private void doProcessSlow(Record record) {
    Iterator<Map.Entry<String, Collection<Object>>> iter = record.getFields().asMap().entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Collection<Object>> entry = iter.next();
      if (nameMatcher.matches(entry.getKey())) {
        List values = (List) entry.getValue();
        for (int i = values.size(); --i >= 0; ) {
          if (valueMatcher.matches(values.get(i).toString())) {
            if (replacement != null) {
              values.set(i, replacement);
            } else if (values.size() > 1) {
              values.remove(i);
            } else {
              iter.remove(); // to avoid ConcurrentModificationException
            }
          }
        }
      }
    }
  }

  private void doProcessFast(Record record) {
    for (String name : nameMatcher.getLiteralsOnly()) {
      List values = record.get(name);
      for (int i = values.size(); --i >= 0; ) {
        if (valueMatcher.matches(values.get(i).toString())) {
          if (replacement != null) {
            values.set(i, replacement);
          } else {
            values.remove(i);
          }
        }
      }
    }
  }

}
