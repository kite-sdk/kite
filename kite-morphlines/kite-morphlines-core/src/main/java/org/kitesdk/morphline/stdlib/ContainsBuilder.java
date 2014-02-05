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
import java.util.List;
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
 * Command that succeeds if one of the field values of the given named field is equal to one of the
 * the given values, and fails otherwise; Multiple fields can be named, in which case the results
 * are ANDed.
 */
public final class ContainsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("contains");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Contains(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Contains extends AbstractCommand {

    private final Set<Map.Entry<String, Object>> entrySet;
    private final String renderedConfig; // cached value
    
    public Contains(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.entrySet = new Configs().getEntrySet(config);
      for (Map.Entry<String, Object> entry : entrySet) {
        if (!(entry.getValue() instanceof Collection)) {
          entry.setValue(new FieldExpression(entry.getValue().toString(), getConfig()));        
        }
      }
      this.renderedConfig = config.root().render();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      for (Map.Entry<String, Object> entry : entrySet) {
        String fieldName = entry.getKey();
        List values = record.get(fieldName);
        Object entryValue = entry.getValue();
        Collection results;
        if (entryValue instanceof Collection) {
          results = (Collection)entryValue;
        } else {
          results = ((FieldExpression) entryValue).evaluate(record);
        }
        boolean found = false;
        for (Object result : results) {
          if (values.contains(result)) {
            found = true;
            break;
          }
        }
        if (!found) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Contains command failed because it could not find any of {} in values: {} for command: {}",
                new Object[]{results, values, renderedConfig});
          }
          return false;
        }
      }
      return super.doProcess(record);
    }
    
  }
  
}
