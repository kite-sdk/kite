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
package com.cloudera.data.partition;

import com.cloudera.data.FieldPartitioner;
import com.google.common.annotations.Beta;

/**
 * Convenience class so you can say, for example, <code>hash("username", 2)</code> in
 * JEXL.
 */
public class PartitionFunctions {

  public static FieldPartitioner hash(String name, int buckets) {
    return new HashFieldPartitioner(name, buckets);
  }

  public static FieldPartitioner identity(String name, int buckets) {
    return new IdentityFieldPartitioner(name, buckets);
  }

  @Beta
  public static FieldPartitioner range(String name, int... upperBounds) {
    return new IntRangeFieldPartitioner(name, upperBounds);
  }

  @Beta
  public static FieldPartitioner range(String name,
      Comparable<?>... upperBounds) {
    return new RangeFieldPartitioner(name, upperBounds);
  }

}
