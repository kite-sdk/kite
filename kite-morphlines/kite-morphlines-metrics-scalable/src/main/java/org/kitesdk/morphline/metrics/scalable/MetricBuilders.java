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

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;

/**
 * Utilities for codahale metrics library.
 */
class MetricBuilders {

  public Counter getCounter(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, COUNTERS);
  }

  public Meter getMeter(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, METERS);
  }

  public Histogram getHistogram(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, HISTOGRAMS);
  }

  public Timer getTimer(MetricRegistry registry, String name) {
    return getOrAdd(registry, name, TIMERS);
  }

  public Histogram getSlidingWindowHistogram(MetricRegistry registry, String name, final int size) {
    return getOrAdd(registry, name, new MetricBuilder<Histogram>() {
      @Override
      public Histogram newMetric() {
        return new Histogram(new SlidingWindowReservoir(size));
      }
      @Override
      public boolean isInstance(Metric metric) {
        return Histogram.class.isInstance(metric);
      }          
    });
  }

  public Histogram getSlidingTimeWindowHistogram(MetricRegistry registry, String name, final long window,
      final TimeUnit windowUnit) {
    return getOrAdd(registry, name, new MetricBuilder<Histogram>() {
      @Override
      public Histogram newMetric() {
        return new Histogram(new SlidingTimeWindowReservoir(window, windowUnit));
      }
      @Override
      public boolean isInstance(Metric metric) {
        return Histogram.class.isInstance(metric);
      }          
    });
  }

  public Timer getSlidingWindowTimer(MetricRegistry registry, String name, final int size) {
    return getOrAdd(registry, name, new MetricBuilder<Timer>() {
      @Override
      public Timer newMetric() {
        return new Timer(new SlidingWindowReservoir(size));
      }
      @Override
      public boolean isInstance(Metric metric) {
        return Timer.class.isInstance(metric);
      }          
    });
  }

  public Timer getSlidingTimeWindowTimer(MetricRegistry registry, String name, final long window,
      final TimeUnit windowUnit) {
    return getOrAdd(registry, name, new MetricBuilder<Timer>() {
      @Override
      public Timer newMetric() {
        return new Timer(new SlidingTimeWindowReservoir(window, windowUnit));
      }
      @Override
      public boolean isInstance(Metric metric) {
        return Timer.class.isInstance(metric);
      }          
    });
  }

  @SuppressWarnings("unchecked")
  protected <T extends Metric> T getOrAdd(MetricRegistry registry, String name, MetricBuilder<T> builder) {
    final Metric metric = registry.getMetrics().get(name);
    if (builder.isInstance(metric)) {
      return (T) metric;
    } else if (metric == null) {
      try {
        return registry.register(name, builder.newMetric());
      } catch (IllegalArgumentException e) {
        final Metric added = registry.getMetrics().get(name);
        if (builder.isInstance(added)) {
          return (T) added;
        }
      }
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  private static final MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
    @Override
    public Counter newMetric() {
      return new Counter();
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Counter.class.isInstance(metric);
    }
  };

  private static final MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
    @Override
    public Meter newMetric() {
      return new Meter();
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Meter.class.isInstance(metric);
    }
  };

  private static final MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
    @Override
    public Histogram newMetric() {
      return new Histogram(new ExponentiallyDecayingReservoir());
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Histogram.class.isInstance(metric);
    }
  };

  private static final MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
    @Override
    public Timer newMetric() {
      return new Timer();
    }

    @Override
    public boolean isInstance(Metric metric) {
      return Timer.class.isInstance(metric);
    }
  };

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  protected interface MetricBuilder<T extends Metric> {

    T newMetric();

    boolean isInstance(Metric metric);
  }
}
