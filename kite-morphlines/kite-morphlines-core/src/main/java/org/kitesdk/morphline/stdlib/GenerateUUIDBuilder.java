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

import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;

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
 * A command that sets a universally unique identifier on all records that are intercepted. By
 * default this event header is named "id".
 */
public final class GenerateUUIDBuilder implements CommandBuilder {

  public static final String FIELD_NAME = "field";
  public static final String PRESERVE_EXISTING_NAME = "preserveExisting";
  public static final String PREFIX_NAME = "prefix";
  
  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("generateUUID");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new GenerateUUID(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class GenerateUUID extends AbstractCommand {
    
    private final String fieldName;
    private final boolean preserveExisting;
    private final String prefix;
    private final RandomGenerator prng;

    public GenerateUUID(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) { 
      super(builder, config, parent, child, context);
      this.fieldName = getConfigs().getString(config, FIELD_NAME, Fields.ID);
      this.preserveExisting = getConfigs().getBoolean(config, PRESERVE_EXISTING_NAME, true);
      this.prefix = getConfigs().getString(config, PREFIX_NAME, "");
      Type type = new Validator<Type>().validateEnum(
          config,
          getConfigs().getString(config, "type", Type.secure.toString()),
          Type.class);
      if (type == Type.secure) {
        prng = null; // secure & slow
      } else {
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
      if (preserveExisting && record.getFields().containsKey(fieldName)) {
        ; // we must preserve the existing id
      } else {
        record.replaceValues(fieldName, generateUUID());
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

    private String generateUUID() {
      UUID uuid;
      if (prng == null) {
        uuid = UUID.randomUUID(); // secure & slow
      } else {
        uuid = new UUID(prng.nextLong(), prng.nextLong()); // non-secure & fast
      }
      return concat(prefix, uuid.toString());
    }

    private String concat(String x, String y) {
      return x.length() == 0 ? y : x + y;
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
