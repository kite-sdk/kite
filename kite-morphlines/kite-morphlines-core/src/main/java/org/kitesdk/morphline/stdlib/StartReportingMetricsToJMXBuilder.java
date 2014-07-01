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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.JmxReporter.Builder;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Command that starts publishing the metrics of the
 * {@code com.codahale.metrics.MetricRegistry} of the morphline context
 * to JMX, configured via a {@code com.codahale.metrics.JmxReporter.Builder}.
 */
public final class StartReportingMetricsToJMXBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("startReportingMetricsToJMX");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new StartReportingMetricsToJMX(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StartReportingMetricsToJMX extends AbstractCommand {

    private final String domain;
    private static final Map<MetricRegistry, Map<String, JmxReporter>> REGISTRIES = Maps.newIdentityHashMap();
    
    public StartReportingMetricsToJMX(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      
      MetricFilter filter = PatternMetricFilter.parse(getConfigs(), config);
      TimeUnit defaultDurationUnit = getConfigs().getTimeUnit(config, "defaultDurationUnit", TimeUnit.MILLISECONDS);
      TimeUnit defaultRateUnit = getConfigs().getTimeUnit(config, "defaultRateUnit", TimeUnit.SECONDS);
      
      Map<String, TimeUnit> durationUnits =  Maps.newHashMap();
      Config durationUnitsConfig = getConfigs().getConfig(config, "durationUnits", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(durationUnitsConfig)) {
        TimeUnit unit = new Configs().getTimeUnit(entry.getValue().toString());
        durationUnits.put(entry.getKey(), unit);
      }      
      Map<String, TimeUnit> rateUnits =  Maps.newHashMap();
      Config rateUnitsConfig = getConfigs().getConfig(config, "rateUnits", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : new Configs().getEntrySet(rateUnitsConfig)) {
        TimeUnit unit = new Configs().getTimeUnit(entry.getValue().toString());
        rateUnits.put(entry.getKey(), unit);
      }            
      this.domain = getConfigs().getString(config, "domain", "metrics");      
      validateArguments();
      
      MetricRegistry registry = context.getMetricRegistry();
      synchronized (REGISTRIES) {
        Map<String, JmxReporter> reporters = REGISTRIES.get(registry);
        if (reporters == null) {
          reporters = Maps.newHashMap();
          REGISTRIES.put(registry, reporters);
        }
        JmxReporter reporter = reporters.get(domain);
        if (reporter == null) {
          Builder reporterBuilder = JmxReporter.forRegistry(registry)
              .filter(filter)
              .convertDurationsTo(defaultDurationUnit)
              .convertRatesTo(defaultRateUnit)
              .specificDurationUnits(durationUnits)
              .specificRateUnits(rateUnits)
              .inDomain(domain);
          
          reporter = reporterBuilder.build();
          reporter.start();
          reporters.put(domain, reporter);
        }
      }
    }
        
    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          synchronized (REGISTRIES) {
            Map<String, JmxReporter> reporters = REGISTRIES.get(getContext().getMetricRegistry());
            if (reporters != null) {
              JmxReporter reporter = reporters.remove(domain);
              if (reporter != null) {
                reporter.stop();
              }
            }
          }
        }
      }
      super.doNotify(notification);
    }
    
  }
  
}
