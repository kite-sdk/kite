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

import com.typesafe.config.Config;

/**
 * Command that silently consumes records without ever emitting any record - think /dev/null.
 */
public final class DropRecordBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("dropRecord");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    if (config == null) {
      return new DevNull(parent);
    } else {
      return new DropRecord(this, config, parent, child, context);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Implementation that does not do logging and metrics */
  private static final class DevNull implements Command {
    
    private Command parent;
    
    public DevNull(Command parent) { 
      this.parent = parent;
    }

    @Override
    public Command getParent() {
      return parent;
    }
    
    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
      return true;
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Implementation that does logging and metrics */
  private static final class DropRecord extends AbstractCommand {
    
    public DropRecord(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
    }

    @Override
    protected boolean doProcess(Record record) {
      return true;
    }

  }

}
