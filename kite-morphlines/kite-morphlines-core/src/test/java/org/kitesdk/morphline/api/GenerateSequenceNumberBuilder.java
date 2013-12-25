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
package org.kitesdk.morphline.api;

import java.util.Collection;
import java.util.Collections;

import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

public final class GenerateSequenceNumberBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("generateSequenceNumber");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new GenerateSequenceNumber(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class GenerateSequenceNumber extends AbstractCommand {

    private final String fieldName;
    private final boolean preserveExisting;
    private long seqNum = 0;
    
    public GenerateSequenceNumber(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.fieldName = getConfigs().getString(config, "field");
      this.preserveExisting = getConfigs().getBoolean(config, "preserveExisting", true);      
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record record) {
      if (preserveExisting && record.getFields().containsKey(fieldName)) {
        // we must preserve the existing id
      } else {
        record.replaceValues(fieldName, seqNum++);
      }
      return super.doProcess(record);
    }
    
  }
  
}
