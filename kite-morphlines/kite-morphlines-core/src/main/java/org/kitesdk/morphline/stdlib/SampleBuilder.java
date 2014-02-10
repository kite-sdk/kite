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

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.shaded.org.apache.commons.math3.random.RandomGenerator;
import org.kitesdk.morphline.shaded.org.apache.commons.math3.random.Well19937c;

import com.typesafe.config.Config;

/**
 * Command that forwards each input record with a given probability to its child command, and
 * silently ignores all other input records. Sampling is based on a random number generator. This
 * can be helpful to easily test a morphline with a random subset of records from a large dataset.
 */
public final class SampleBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("sample");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Sample(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Sample extends AbstractCommand {

    private final double probability;
    private final RandomGenerator prng;
    private long count = 0;
    
    public Sample(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);    
      this.probability = getConfigs().getDouble(config, "probability", 1.0);
      if (probability < 0.0) {
        throw new MorphlineCompilationException("Probability must not be negative: " + probability, config);
      }
      if (probability >= 1.0) {
        this.prng = null;
      } else {
        if (config.hasPath("seed")) {
          long seed = getConfigs().getLong(config, "seed");
          this.prng = new Well19937c(seed); // non-secure & fast
        } else {
          Random rand = new SecureRandom();
          int[] seed = new int[624];
          for (int i = 0; i < seed.length; i++) {
            seed[i] = rand.nextInt();
          }
          this.prng = new Well19937c(seed); // non-secure & fast
        }
      }
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {      
      if (prng != null && prng.nextDouble() > probability) {
        return true; // silently ignore this record
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("sampleCount: {}", count);
      }
      count++;
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }
    
  }
  
}
