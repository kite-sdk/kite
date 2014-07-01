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
import java.util.ListIterator;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * Command that converts the Java objects in a given field via <code>Object.toString()</code> to
 * their string representation, and optionally also applies <code>String.trim()</code>.
 */
public final class ToStringBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("toString");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ToString(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ToString extends AbstractCommand {

    private final String fieldName;
    private final boolean trim;
    
    public ToString(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      this.fieldName = getConfigs().getString(config, "field");
      this.trim = getConfigs().getBoolean(config, "trim", false);
      validateArguments();
    }
        
    @Override
    @SuppressWarnings("unchecked")
    protected boolean doProcess(Record record) {
      ListIterator iter = record.get(fieldName).listIterator();
      while (iter.hasNext()) {
        String str = iter.next().toString();
        if (trim) {
          str = str.trim();
        }
        iter.set(str);
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
  }
  
}
