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

import com.codahale.metrics.Timer;

/**
 * A timer that autocorrects for the "Coordinated Omission Problem" described at
 * https://groups.google.com/forum/#!msg/mechanical-sympathy/icNZJejUHfE/BfDekfBEs_sJ and
 * https://www.youtube.com/watch?v=lJ8ydIuPFeU in the same way as Gil Tene's HdrHistogram per
 * https://github.com/HdrHistogram/HdrHistogram
 * 
 * The corrected values are forwarded to an underlying codahale metrics {@link Timer}. This class
 * also maintains a timer that does not apply autocorrection so that corrected vs. uncorrected
 * values are always available and can always be plotted and compared.
 */
final class CoordinatedOmissionTimer {
  
  private Timer uncorrectedTimer;
  private Timer correctedTimer;
  private long expectedNanoIntervalBetweenSamples;
  
  public CoordinatedOmissionTimer(Timer uncorrectedTimer, Timer correctedTimer, long expectedNanoIntervalBetweenSamples) {
    this.uncorrectedTimer = uncorrectedTimer;
    this.correctedTimer = correctedTimer;
    if (expectedNanoIntervalBetweenSamples < 0) {
      throw new IllegalArgumentException(
          "expectedNanoIntervalBetweenSamples must be >= 0: " + expectedNanoIntervalBetweenSamples);
    }
    this.expectedNanoIntervalBetweenSamples = expectedNanoIntervalBetweenSamples;
  }
  
  public void update(long duration, TimeUnit unit) {
    long nanoDuration = unit.toNanos(duration);
    uncorrectedTimer.update(nanoDuration, TimeUnit.NANOSECONDS);
    if (correctedTimer != null) {
      correctedTimer.update(nanoDuration, TimeUnit.NANOSECONDS);    
      long interval = expectedNanoIntervalBetweenSamples;
      if (interval > 0) {
        for (long missingDuration = nanoDuration - interval; missingDuration >= interval; missingDuration -= interval) {
          correctedTimer.update(missingDuration, TimeUnit.NANOSECONDS);
        }
      }
    }
  }

}
