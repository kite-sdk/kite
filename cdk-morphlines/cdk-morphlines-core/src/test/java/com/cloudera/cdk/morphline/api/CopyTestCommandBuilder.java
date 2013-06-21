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
package com.cloudera.cdk.morphline.api;

import java.util.Collection;
import java.util.Collections;

import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

public final class CopyTestCommandBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("copyTest");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new CopyTestCommand(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class CopyTestCommand extends AbstractCommand {

    private String name;
    private int count;
    
    public CopyTestCommand(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.name = getConfigs().getString(config, "name");
      this.count = getConfigs().getInt(config, "count", 2);
    }
    
    @Override
    protected boolean doProcess(Record record) {
      for (int i = 0; i < count; i++) {
        Record next = record.copy();
        next.replaceValues(name, i);
        if (!getChild().process(next)) {
          return false;
        }
      }
      return true;
    }
    
  }
  
}
