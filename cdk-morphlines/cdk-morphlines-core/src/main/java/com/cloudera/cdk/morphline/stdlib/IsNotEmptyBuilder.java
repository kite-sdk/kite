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

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.typesafe.config.Config;

/**
 * TODO
 */
final class IsNotEmptyBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("isNotEmpty");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new IsNotEmpty(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class IsNotEmpty extends AbstractCommand {

    private final String fieldName;
    
    public IsNotEmpty(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      this.fieldName = Configs.getString(config, "field");
    }
        
    @Override
    protected boolean doProcess(Record record) {      
      if (!record.getFields().containsKey(fieldName)) {
        return false;
      }
      return super.doProcess(record);
    }
    
  }
  
}
