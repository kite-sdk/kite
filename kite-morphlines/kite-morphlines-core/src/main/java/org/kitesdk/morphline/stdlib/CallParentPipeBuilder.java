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

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

/**
 * Command that routes records to the enclosing pipe morphline object.
 */
public final class CallParentPipeBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("callParentPipe");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new CallParentPipe(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class CallParentPipe extends AbstractCommand {

    public CallParentPipe(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, getPipe(parent), context);
      validateArguments();
    }
    
    @Override
    protected void doNotify(Record notification) {
      ; // don't forward to avoid endless loops
    }
    
    private static Pipe getPipe(Command p) {
      while (!(p instanceof Pipe)) {
        p = p.getParent();
      }
      Preconditions.checkNotNull(p);
      return (Pipe) p;
    }

  }
  
}
