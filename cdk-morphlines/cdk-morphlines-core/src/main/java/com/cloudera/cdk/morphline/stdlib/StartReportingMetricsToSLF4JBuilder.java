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
package com.cloudera.cdk.morphline.stdlib;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.slf4j.helpers.BasicMarkerFactory;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Slf4jReporter.Builder;
import com.typesafe.config.Config;

/**
 * Command that starts periodically logging the metrics of the
 * {@code com.codahale.metrics.MetricRegistry} of the
 * morphline context to SLF4j, configured via a
 * {@code com.codahale.metrics.Slf4jReporter.Builder}.
 */
public final class StartReportingMetricsToSLF4JBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("startReportingMetricsToSLF4J");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new StartReportingMetricsToSLF4J(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StartReportingMetricsToSLF4J extends AbstractCommand {

    private final String logger;
    private static final Map<MetricRegistry, Map<String, Slf4jReporter>> REGISTRIES = new IdentityHashMap();
    
    public StartReportingMetricsToSLF4J(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      
      MetricFilter filter = PatternMetricFilter.parse(getConfigs(), config);
      TimeUnit defaultDurationUnit = getConfigs().getTimeUnit(config, "defaultDurationUnit", TimeUnit.MILLISECONDS);
      TimeUnit defaultRateUnit = getConfigs().getTimeUnit(config, "defaultRateUnit", TimeUnit.SECONDS); 
      long period = getConfigs().getNanoseconds(config, "period", 10 * 1000L * 1000 * 1000); // 10 secs, also see https://github.com/typesafehub/config/blob/master/HOCON.md#duration-format
      this.logger = getConfigs().getString(config, "logger", "metrics");
      String marker = getConfigs().getString(config, "marker", null);      
      validateArguments();
      
      MetricRegistry registry = context.getMetricRegistry();
      synchronized (REGISTRIES) {
        Map<String, Slf4jReporter> reporters = REGISTRIES.get(registry);
        if (reporters == null) {
          reporters = new HashMap();
          REGISTRIES.put(registry, reporters);
        }
        Slf4jReporter reporter = reporters.get(logger);
        if (reporter == null) {
          Builder builder = Slf4jReporter.forRegistry(registry)
              .filter(filter)
              .convertDurationsTo(defaultDurationUnit)
              .convertRatesTo(defaultRateUnit)
              .outputTo(LoggerFactory.getLogger(logger));
          
          if (marker != null) {
            builder = builder.markWith(new BasicMarkerFactory().getMarker(marker));
          }
              
          reporter = builder.build();
          reporter.start(period, TimeUnit.NANOSECONDS);
          reporters.put(logger, reporter);
        }
      }
    }
        
    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          synchronized (REGISTRIES) {
            Map<String, Slf4jReporter> reporters = REGISTRIES.get(getContext().getMetricRegistry());
            if (reporters != null) {
              Slf4jReporter reporter = reporters.remove(logger);
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
