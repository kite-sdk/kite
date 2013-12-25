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
package org.kitesdk.morphline.metrics.servlets;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.base.AbstractCommand;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.jvm.ThreadDeadlockHealthCheck;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.typesafe.config.Config;

/**
 * Command that registers metrics that are related to the Java Virtual Machine with the
 * MorphlineContext of the morphline. For example, this includes metrics for garbage collection
 * events, buffer pools, threads and thread deadlocks.
 */
public final class RegisterJVMMetricsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("registerJVMMetrics");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new RegisterJVMMetrics(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RegisterJVMMetrics extends AbstractCommand {

    public RegisterJVMMetrics(CommandBuilder builder, Config config, Command parent, 
                                       Command child, final MorphlineContext context) {
      
      super(builder, config, parent, child, context);      
      validateArguments();
      
      MetricRegistry registry = context.getMetricRegistry();
      BufferPoolMetricSet bufferPoolMetrics = new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer());
      registerAll("jvm.buffers", bufferPoolMetrics, registry);
      registerAll("jvm.gc", new GarbageCollectorMetricSet(), registry);
      registerAll("jvm.memory", new MemoryUsageGaugeSet(), registry);
      registerAll("jvm.threads", new ThreadStatesGaugeSet(), registry);
      register("jvm.fileDescriptorCountRatio", new FileDescriptorRatioGauge(), registry);
      context.getHealthCheckRegistry().register("deadlocks", new ThreadDeadlockHealthCheck());
    }
        
    // Same method as registry.registerAll(prefix, metrics) except that it avoids an exception 
    // on registering the same metric more than once
    private void registerAll(String prefix, MetricSet metrics, MetricRegistry registry) {
      for (Map.Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
        String name = MetricRegistry.name(prefix, entry.getKey());
        if (entry.getValue() instanceof MetricSet) {
          registerAll(name, (MetricSet) entry.getValue(), registry);
        } else {
          register(name, entry.getValue(), registry);
        }
      } 
    }

    // avoids an exception on registering the same metric more than once
    private void register(String name, Metric metric, MetricRegistry registry) {
      if (!registry.getMetrics().containsKey(name)) { // this check is the diff
        try {
          registry.register(name, metric);
        } catch (IllegalArgumentException e) { 
          // can happen because there is a small window for a race
          String msg = "A metric named " + name + " already exists";
          if (!msg.equals(e.getMessage())) {
            throw e; // exception wasn't caused by said race
          }
        }
      }
    }
    
  }
  
}
