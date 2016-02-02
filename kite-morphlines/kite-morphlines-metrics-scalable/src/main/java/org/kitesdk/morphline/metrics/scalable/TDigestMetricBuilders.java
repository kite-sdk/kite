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
package org.kitesdk.morphline.metrics.scalable;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Convenience classes for codahale metrics using aggregations based on t-digest algorithm.
 */
class TDigestMetricBuilders extends MetricBuilders {
  
  @Override
  public Histogram getHistogram(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, HISTOGRAMS);
  }
  
  @Override
  public Timer getTimer(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, TIMERS);
  }
  
  private static final MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
    @Override
    public Histogram newMetric() {
      return new Histogram(new TDigestReservoir());
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Histogram.class.isInstance(metric);
    }
  };

  private static final MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
    @Override
    public Timer newMetric() {
      return new Timer(new TDigestReservoir());
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Timer.class.isInstance(metric);
    }
  };
 
}
