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
package org.kitesdk.data.crunch.morphline;

import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.ListIterator;
import java.util.Random;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;
import org.kitesdk.morphline.shaded.org.apache.commons.math3.random.RandomGenerator;
import org.kitesdk.morphline.shaded.org.apache.commons.math3.random.Well19937c;

import com.typesafe.config.Config;

/**
 * A command that prepends a 32 bit universally unique identifier on all records that are intercepted. By
 * default this event header is named "id".
 */
public final class GenerateUUIDPrefixWith32BitBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("generateUUIDPrefixWith32Bit");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new GenerateUUIDPrefixWith32Bit(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class GenerateUUIDPrefixWith32Bit extends AbstractCommand {
    
    private final String fieldName;
    private final RandomGenerator prng;
    private final SecureRandom secure;

    public GenerateUUIDPrefixWith32Bit(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) { 
      super(builder, config, parent, child, context);
      this.fieldName = getConfigs().getString(config, "field", Fields.ID);
      Type type = new Validator<Type>().validateEnum(
          config,
          getConfigs().getString(config, "type", Type.secure.toString()),
          Type.class);
      if (type == Type.secure) {
        prng = null; // secure & slow
        secure = new SecureRandom();
      } else {
        secure = null;
        Random rand = new SecureRandom();
        int[] seed = new int[624];
        for (int i = 0; i < seed.length; i++) {
          seed[i] = rand.nextInt();
        }
        prng = new Well19937c(seed); // non-secure & fast
      }
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record) {
      ListIterator iter = record.get(fieldName).listIterator();
      if (iter.hasNext()) {
        while (iter.hasNext()) {
          iter.set(generateUUID() + "@" + iter.next());
        }
      } else {
        record.put(fieldName, generateUUID());
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
    private String generateUUID() {
      int uuid;
      if (secure != null) {
        uuid = secure.nextInt(); // secure & slow
      } else {
        uuid = prng.nextInt(); // non-secure & fast
      }
      return String.format("%08x", uuid);
    }

  }

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static enum Type {
    secure,
    nonSecure
  }     

}
