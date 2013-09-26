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

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.CsvReporter.Builder;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.typesafe.config.Config;

/**
 * Command that starts periodically appending the metrics of the
 * {@code com.codahale.metrics.MetricRegistry} of the
 * morphline context to a set of CSV files, configured via a
 * {@code com.codahale.metrics.CsvReporter.Builder}. The CSV
 * files are named after the metrics.
 */
public final class StartReportingMetricsToCSVBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("startReportingMetricsToCSV");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new StartReportingMetricsToCSV(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StartReportingMetricsToCSV extends AbstractCommand {

    private final File dir;
    private static final Map<MetricRegistry, Map<File, CsvReporter>> REGISTRIES = new IdentityHashMap();
    
    public StartReportingMetricsToCSV(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      
      MetricFilter filter = PatternMetricFilter.parse(getConfigs(), config);
      TimeUnit defaultDurationUnit = getConfigs().getTimeUnit(config, "defaultDurationUnit", TimeUnit.MILLISECONDS);
      TimeUnit defaultRateUnit = getConfigs().getTimeUnit(config, "defaultRateUnit", TimeUnit.SECONDS); 
      long period = getConfigs().getNanoseconds(config, "period", 10 * 1000L * 1000 * 1000); // 10 seconds
      if (LOG.isTraceEnabled()) {
        LOG.trace("availableLocales: {}", Joiner.on("\n").join(Locale.getAvailableLocales()));
      }
      Locale locale = getConfigs().getLocale(config, "locale", Locale.getDefault());
      this.dir = new File(getConfigs().getString(config, "dir"));
      validateArguments();
      
      MetricRegistry registry = context.getMetricRegistry();
      synchronized (REGISTRIES) {
        Map<File, CsvReporter> reporters = REGISTRIES.get(registry);
        if (reporters == null) {
          reporters = new HashMap();
          REGISTRIES.put(registry, reporters);
        }
        CsvReporter reporter = reporters.get(dir);
        if (reporter == null) {
          Builder builder = CsvReporter.forRegistry(registry)
              .filter(filter)
              .convertDurationsTo(defaultDurationUnit)
              .convertRatesTo(defaultRateUnit)
              .formatFor(locale);
              
          reporter = builder.build(dir);
          dir.mkdirs();
          if (!dir.isDirectory()) {
            throw new MorphlineCompilationException("Directory not found: " + dir, config);
          }
          if (!dir.canWrite()) {
            throw new MorphlineCompilationException("Directory not writeable: " + dir, config);
          }
          reporter.start(period, TimeUnit.NANOSECONDS);
          reporters.put(dir, reporter);
        }
      }
    }
        
    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          synchronized (REGISTRIES) {
            Map<File, CsvReporter> reporters = REGISTRIES.get(getContext().getMetricRegistry());
            if (reporters != null) {
              CsvReporter reporter = reporters.remove(dir);
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
