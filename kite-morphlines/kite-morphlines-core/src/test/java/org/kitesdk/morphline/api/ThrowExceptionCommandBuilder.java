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

import com.typesafe.config.Config;

public final class ThrowExceptionCommandBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("throwException");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ThrowExceptionCommand(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ThrowExceptionCommand implements Command {
    
    private Command parent;
    private Command child;
    
    public ThrowExceptionCommand(Config config, Command parent, Command child, MorphlineContext context) { 
      this.parent = parent;
      this.child = child;
    }

    @Override
    public Command getParent() {
      return parent;
    }
    
    @Override
    public void notify(Record notification) {
      child.notify(notification);
    }

    @Override
    public boolean process(Record record) {
      throw new MorphlineRuntimeException("Forced exception");
    }

  }

}
