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
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import com.typesafe.config.Config;

/**
 * Command that ignores all input records beyond the N-th record, thus emitting at most N records,
 * akin to the Unix <code>head</code> command. This can be helpful to quickly test a morphline with
 * the first few records from a larger dataset.
 */
public final class HeadBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("head");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Head(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Head extends AbstractCommand {

    private final long limit;
    private long count = 0;
    
    public Head(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);    
      this.limit = getConfigs().getLong(config, "limit", -1);
      if (limit < -1) {
        throw new MorphlineCompilationException("Illegal limit: " + limit, config);
      }
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      if (limit >= 0 && count >= limit) {        
        return true; // silently ignore this record
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("headCount: {}", count);
      }
      count++;
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
  }
  
}
