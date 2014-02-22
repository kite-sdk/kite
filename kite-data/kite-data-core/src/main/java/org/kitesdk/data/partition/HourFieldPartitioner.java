/**
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
package org.kitesdk.data.partition;

import java.text.NumberFormat;

/**
 * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"EQ_DOESNT_OVERRIDE_EQUALS",
           "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
           "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification="Replaced by parent class")
public class HourFieldPartitioner extends
    org.kitesdk.data.spi.partition.HourFieldPartitioner {
  private final NumberFormat format;

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public HourFieldPartitioner(String sourceName, String name) {
    super(sourceName, name);
    format = NumberFormat.getIntegerInstance();
    format.setMinimumIntegerDigits(2);
    format.setMaximumIntegerDigits(2);
  }

  @Override
  @Deprecated
  public String valueToString(Integer value) {
    return format.format(value);
  }
}
