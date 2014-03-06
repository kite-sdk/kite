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

/**
 * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
           "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS"},
    justification="Replaced by parent class")
public class IdentityFieldPartitioner<S extends Comparable> extends
    org.kitesdk.data.spi.partition.IdentityFieldPartitioner<S> {

  /**
   * @deprecated will be removed in 0.13.0; moved to package org.kitesdk.data.spi
   */
  @Deprecated
  public IdentityFieldPartitioner(String name, Class<S> type, int buckets) {
    super(name, name + "_copy", type, buckets);
  }

  @Override
  @Deprecated
  @SuppressWarnings("unchecked")
  public S valueFromString(String stringValue) {
    if (getType() == Integer.class) {
      return (S) Integer.valueOf(stringValue);
    } else if (getType() == Long.class) {
      return (S) Long.valueOf(stringValue);
    } else if (getType() == String.class) {
      return (S) stringValue;
    }
    throw new IllegalArgumentException("Cannot convert string to type " + getType());
  }
}
