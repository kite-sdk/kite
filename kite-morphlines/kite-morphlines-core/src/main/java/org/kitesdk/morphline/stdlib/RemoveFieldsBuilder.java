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
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * Removes all record fields for which the field name matches at least one of the given blacklist
 * predicates but none of the given whitelist predicates.
 * 
 * If the blacklist specification is absent it defaults to MATCH ALL. If the whitelist specification
 * is absent it defaults to MATCH NONE.
 */
public final class RemoveFieldsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("removeFields");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new RemoveFields(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RemoveFields extends AbstractCommand {
    
    private final PatternNameMatcher nameMatcher;

    public RemoveFields(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) { 
      super(builder, config, parent, child, context);
      List<String> includes = getConfigs().getStringList(config, "blacklist", Collections.singletonList("*"));
      List<String> excludes = getConfigs().getStringList(config, "whitelist", Collections.<String>emptyList());
      int cacheCapacity = getConfigs().getInt(config, "cacheCapacity", 10000);
      this.nameMatcher = new PatternNameMatcher(includes, excludes, cacheCapacity);
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
      Iterator<String> iter = record.getFields().asMap().keySet().iterator();
      while (iter.hasNext()) {
        if (nameMatcher.matches(iter.next())) {
          iter.remove();
        }
      }
    }

    private void doProcessFast(Record record) {
      for (String name : nameMatcher.getLiteralsOnly()) {
        record.removeAll(name);
      }
    }

  }

}
