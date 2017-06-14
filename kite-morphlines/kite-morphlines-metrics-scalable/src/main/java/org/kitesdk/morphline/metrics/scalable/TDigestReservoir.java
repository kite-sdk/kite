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

import java.io.OutputStream;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

/**
 * Enables codahale {@link Histogram} and {@link Timer} metrics to use
 * {@link ScalableStatistics} as a backing data structure.
 */
public final class TDigestReservoir implements Reservoir {

  private final ScalableStatistics stats;
  
  public TDigestReservoir() {
    this(new ScalableStatistics());
  }

  public TDigestReservoir(ScalableStatistics stats) {
    Preconditions.checkNotNull(stats);
    this.stats = stats;
  }
  
  @Override
  synchronized public int size() {
    return (int) Math.min(stats.getCount(), Integer.MAX_VALUE);
  }

  @Override
  synchronized public void update(long value) {
    stats.add(value);
  }

  @Override
  synchronized public Snapshot getSnapshot() {
    return new TDigestSnapshot(stats.copy());
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class TDigestSnapshot extends Snapshot {

    private final ScalableStatistics stats;
    
    private TDigestSnapshot(ScalableStatistics stats) {
      super();
      this.stats = stats;
    }
    
    public ScalableStatistics getScalableStatistics() {
      return stats;
    }
        
    @Override
    public int size() {
      return (int) Math.min(stats.getCount(), Integer.MAX_VALUE);
    }

    @Override
    public long getMax() {
      if (stats.getCount() == 0) {
        return 0;
      }
      return Math.round(stats.getMax());
    }

    @Override
    public long getMin() {
      if (stats.getCount() == 0) {
        return 0;
      }
      return Math.round(stats.getMin());
    }

    @Override
    public double getMean() {
      if (stats.getCount() == 0) {
        return 0.0;
      }
      return stats.getMean();
    }

    @Override
    public double getStdDev() {
      if (stats.getCount() == 0) {
        return 0.0;
      }
      return stats.getStandardDeviation();
    }

    @Override
    public double getValue(double quantile) {
      if (stats.getCount() == 0) {
        return 0.0;
      }
      return stats.getQuantile(quantile);
    }
    
    @Override
    public long[] getValues() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dump(OutputStream output) {
      throw new UnsupportedOperationException();
    }

  }
}
