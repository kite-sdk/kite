/**
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.FieldExpression;
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
    return new Contains(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Contains extends AbstractCommand {

    private final Set<Map.Entry<String, Object>> entrySet;
    
    public Contains(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      entrySet = config.root().unwrapped().entrySet();
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
          results = new FieldExpression(entryValue.toString(), getConfig()).evaluate(record);
        }
        boolean found = false;
        for (Object result : results) {
          if (values.contains(result)) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
      return super.doProcess(record);
    }
    
  }
  
}
