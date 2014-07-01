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

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.CsvReporter.Builder;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
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
    return new StartReportingMetricsToCSV(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StartReportingMetricsToCSV extends AbstractCommand {

    private final File outputDir;
    private static final Map<MetricRegistry, Map<File, CsvReporter>> REGISTRIES = Maps.newIdentityHashMap();
    
    public StartReportingMetricsToCSV(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);      
      
      MetricFilter filter = PatternMetricFilter.parse(getConfigs(), config);
      TimeUnit defaultDurationUnit = getConfigs().getTimeUnit(config, "defaultDurationUnit", TimeUnit.MILLISECONDS);
      TimeUnit defaultRateUnit = getConfigs().getTimeUnit(config, "defaultRateUnit", TimeUnit.SECONDS); 
      long frequency = getConfigs().getNanoseconds(config, "frequency", 10 * 1000L * 1000 * 1000); // 10 seconds
      if (LOG.isTraceEnabled()) {
        LOG.trace("availableLocales: {}", Joiner.on("\n").join(Locale.getAvailableLocales()));
      }
      Locale locale = getConfigs().getLocale(config, "locale", Locale.getDefault());
      this.outputDir = new File(getConfigs().getString(config, "outputDir"));
      validateArguments();
      
      MetricRegistry registry = context.getMetricRegistry();
      synchronized (REGISTRIES) {
        Map<File, CsvReporter> reporters = REGISTRIES.get(registry);
        if (reporters == null) {
          reporters = Maps.newHashMap();
          REGISTRIES.put(registry, reporters);
        }
        CsvReporter reporter = reporters.get(outputDir);
        if (reporter == null) {
          Builder reporterBuilder = CsvReporter.forRegistry(registry)
              .filter(filter)
              .convertDurationsTo(defaultDurationUnit)
              .convertRatesTo(defaultRateUnit)
              .formatFor(locale);
              
          reporter = reporterBuilder.build(outputDir);
          outputDir.mkdirs();
          if (!outputDir.isDirectory()) {
            throw new MorphlineCompilationException("Directory not found: " + outputDir, config);
          }
          if (!outputDir.canWrite()) {
            throw new MorphlineCompilationException("Directory not writeable: " + outputDir, config);
          }
          reporter.start(frequency, TimeUnit.NANOSECONDS);
          reporters.put(outputDir, reporter);
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
              CsvReporter reporter = reporters.remove(outputDir);
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
