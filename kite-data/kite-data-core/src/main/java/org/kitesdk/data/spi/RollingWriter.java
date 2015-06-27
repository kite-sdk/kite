/*
 * Copyright 2015 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

/**
 * Datasets writers that can perform time-based and size-based rolling should
 * implement this interface to allow callers to configure the current roll
 * settings and to provide a clock signal for time-based rolling.
 */
public interface RollingWriter extends ClockReady {
  void setRollIntervalMillis(long rollIntervalMillis);
  void setTargetFileSize(long targetSizeBytes);
}
